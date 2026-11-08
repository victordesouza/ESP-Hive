#include "ESPHive.h"
#include <esp_wifi.h>
#include <esp_now.h>
#include <esp_timer.h>
#include <WiFiUdp.h>

// Instâncias globais singleton
ESPHiveClass Hive;
ESPHiveClass ESPHive; // Alias para quem preferir ESPHive.begin()

// Implementação fraca padrão da ApplicationTask
extern "C" void __attribute__((weak)) ApplicationTask(void *parameter)
{
    (void)parameter;
}

// Auxiliar para conversão de milissegundos para ticks
static inline TickType_t MsToTicks(uint32_t ms)
{
    TickType_t t = pdMS_TO_TICKS(ms);
    return (t == 0) ? 1 : t;
}

// Constante de Broadcast MAC para ESP-NOW
static const uint8_t kEspNowBroadcastMac[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

// Filas globais do transporte ESP-NOW
static QueueHandle_t s_espnowDataQ      = nullptr;
static QueueHandle_t s_espnowControlQ   = nullptr;
static QueueHandle_t s_espnowDiscoveryQ = nullptr;

// UDP Sockets para modo Wi-Fi
static WiFiUDP s_udpDataRx;
static WiFiUDP s_udpDataTx;
static WiFiUDP s_udpDiscoveryRx;
static WiFiUDP s_udpDiscoveryTx;
static WiFiUDP s_udpControlRx;
static WiFiUDP s_udpControlTx;

// ============================================================
// CALLBACKS ESP-NOW (Roteamento de pacotes para filas)
// ============================================================
void EspNowHandleRxStatic(const uint8_t *mac, const uint8_t *data, int len)
{
    if (mac == nullptr || data == nullptr) return;
    if (len < 2 || len > (int)(1 + sizeof(Message))) return;

    QueueHandle_t q = nullptr;
    switch (data[0])
    {
    case HIVE_ESPNOW_TAG_DATA:      q = s_espnowDataQ;      break;
    case HIVE_ESPNOW_TAG_CONTROL:   q = s_espnowControlQ;   break;
    case HIVE_ESPNOW_TAG_DISCOVERY: q = s_espnowDiscoveryQ; break;
    default: return;
    }

    if (q == nullptr) return;

    EspNowRxPacket p;
    memcpy(p.mac, mac, 6);
    p.len = (uint16_t)(len - 1);
    memcpy(p.data, data + 1, len - 1);

    if (xQueueSend(q, &p, 0) != pdTRUE)
    {
        Hive._espnowRxDrop++;
    }
}

#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
static void EspNowRecvCallback(const esp_now_recv_info_t *info,
                               const uint8_t *data, int len)
{
    if (info != nullptr) EspNowHandleRxStatic(info->src_addr, data, len);
}
#else
static void EspNowRecvCallback(const uint8_t *mac, const uint8_t *data, int len)
{
    EspNowHandleRxStatic(mac, data, len);
}
#endif

void EspNowSendHandleStatic(const uint8_t *destMac, uint8_t status)
{
    if (destMac != nullptr && memcmp(destMac, kEspNowBroadcastMac, 6) == 0) return;

    if (status == ESP_NOW_SEND_SUCCESS)
        Hive._macTxOk++;
    else
        Hive._macTxFail++;
}

#if defined(ESP_IDF_VERSION) && defined(ESP_IDF_VERSION_VAL) && \
    (ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 5, 0))
static void EspNowSendCallback(const esp_now_send_info_t *info, esp_now_send_status_t status)
{
    EspNowSendHandleStatic(info ? info->des_addr : nullptr, (uint8_t)status);
}
#else
static void EspNowSendCallback(const uint8_t *mac, esp_now_send_status_t status)
{
    EspNowSendHandleStatic(mac, (uint8_t)status);
}
#endif

// ============================================================
// CONSTRUTOR DA CLASSE ESPHIVE
// ============================================================
ESPHiveClass::ESPHiveClass() :
    _initialized(false),
    _kernelReady(false),
    _appTaskFunc(nullptr),
    _timerOvhUs(0.0),
    _taskContexts(nullptr),
    _barrierArrivedGen(nullptr),
    _barrierWaitGen(nullptr),
    _coordGeneration(0),
    _nodeIP(nullptr),
    _nodeMAC(nullptr),
    _nodeDiscovered(nullptr),
    _lastDeliveredSeq(nullptr),
    _linkStats(nullptr),
    _peerSnap(nullptr),
    _peerRun(nullptr),
    _peerRunGen(nullptr),
    _senderQueue(nullptr),
    _dataTxMutex(nullptr),
    _controlTxMutex(nullptr),
    _discoveryTxMutex(nullptr),
    _barrierMutex(nullptr),
    _dataAckSemaphore(nullptr),
    _createAckSemaphore(nullptr),
    _startAckSemaphore(nullptr),
    _nextSequence(0),
    _lastAckSeq(0),
    _lastCreateAckRank(-1),
    _lastCreateAckStatus(0),
    _lastStartAckStatus(0),
    _espnowRxDrop(0),
    _macTxOk(0),
    _macTxFail(0),
    _lastLoopStats(0),
    _lastLoopStackCheck(0)
{
    _sequenceMux = portMUX_INITIALIZER_UNLOCKED;
}

// ============================================================
// INICIALIZAÇÃO DO CLUSTER
// ============================================================
bool ESPHiveClass::begin(int nodeId, int numNodes, int totalTasks, HiveCommMode commMode)
{
    HiveConfig cfg;
    cfg.nodeId = nodeId;
    cfg.numNodes = numNodes;
    cfg.totalTasks = totalTasks;
    cfg.commMode = commMode;
    return begin(cfg);
}

bool ESPHiveClass::begin(const HiveConfig &cfg)
{
    if (_initialized) return true;

    _config = cfg;

    if (_config.nodeId < 0 || _config.nodeId >= _config.numNodes)
    {
        Serial.printf("[KERNEL] ERRO: nodeId (%d) invalido para numNodes (%d)\n",
                      _config.nodeId, _config.numNodes);
        return false;
    }

    if (_config.totalTasks < _config.numNodes)
    {
        Serial.println("[KERNEL] AVISO: totalTasks menor que numNodes. Ajustando para numNodes.");
        _config.totalTasks = _config.numNodes;
    }

    Serial.println();
    Serial.println("==========================================");
    Serial.printf(" ESP-HIVE DISTRIBUTED CLUSTER - NODE %d/%d\n", _config.nodeId, _config.numNodes);
    Serial.println("==========================================");

    calibrateTimer();

    // Alocação das estruturas de dados dinamicamente conforme configuração
    _taskContexts      = (TaskContext*)calloc(_config.totalTasks, sizeof(TaskContext));
    _barrierArrivedGen = (int32_t*)malloc(_config.totalTasks * sizeof(int32_t));
    _barrierWaitGen    = (int32_t*)malloc(_config.totalTasks * sizeof(int32_t));

    _nodeIP            = new IPAddress[_config.numNodes];
    _nodeMAC           = (uint8_t(*)[6])calloc(_config.numNodes, 6);
    _nodeDiscovered    = (bool*)calloc(_config.numNodes, sizeof(bool));
    _lastDeliveredSeq  = (int64_t*)malloc(_config.numNodes * sizeof(int64_t));
    _linkStats         = (LinkStats*)calloc(_config.numNodes, sizeof(LinkStats));
    _peerSnap          = (PeerSnapshot*)calloc(_config.numNodes, sizeof(PeerSnapshot));
    _peerRun           = (PeerSnapshot*)calloc(_config.numNodes, sizeof(PeerSnapshot));
    _peerRunGen        = (int32_t*)malloc(_config.numNodes * sizeof(int32_t));

    if (!_taskContexts || !_barrierArrivedGen || !_barrierWaitGen || !_nodeIP ||
        !_nodeMAC || !_nodeDiscovered || !_lastDeliveredSeq || !_linkStats ||
        !_peerSnap || !_peerRun || !_peerRunGen)
    {
        fatalRestart("falha na alocacao de memoria do cluster");
        return false;
    }

    for (int i = 0; i < _config.totalTasks; i++)
    {
        _taskContexts[i].rank         = i;
        _taskContexts[i].taskHandle   = nullptr;
        _taskContexts[i].receiveQueue = nullptr;
        _taskContexts[i].barrierSem   = nullptr;
        _taskContexts[i].barrierGen   = 0;
        _taskContexts[i].pending      = nullptr;
        _taskContexts[i].pendingCount = 0;
        _taskContexts[i].finalized    = false;

        _barrierArrivedGen[i] = -1;
        _barrierWaitGen[i]    = -1;
    }

    _coordGeneration = 0;
    _nextSequence    = 0;

    for (int i = 0; i < _config.numNodes; i++)
    {
        _nodeDiscovered[i]   = false;
        _nodeIP[i]           = IPAddress(0, 0, 0, 0);
        memset(_nodeMAC[i], 0, 6);
        _lastDeliveredSeq[i] = -1;
        _peerRunGen[i]       = -1;
    }

    resetStats();

    _dataTxMutex      = xSemaphoreCreateMutex();
    _controlTxMutex   = xSemaphoreCreateMutex();
    _discoveryTxMutex = xSemaphoreCreateMutex();
    _barrierMutex     = xSemaphoreCreateMutex();

    _dataAckSemaphore   = xSemaphoreCreateBinary();
    _createAckSemaphore = xSemaphoreCreateBinary();
    _startAckSemaphore  = xSemaphoreCreateBinary();

    _senderQueue = xQueueCreate(HIVE_SENDER_QUEUE_LENGTH, sizeof(Message));

    if (!_dataTxMutex || !_controlTxMutex || !_discoveryTxMutex || !_barrierMutex ||
        !_dataAckSemaphore || !_createAckSemaphore || !_startAckSemaphore ||
        !_senderQueue)
    {
        fatalRestart("falha criando filas/semaforos");
        return false;
    }

    printLocalRanks();

    // Inicialização da camada física de rádio/rede
    if (_config.numNodes == 1)
    {
        Serial.println("[COMM] Modo: STANDALONE (numNodes=1) - operacao local");
        _nodeDiscovered[_config.nodeId] = true;
    }
    else
    {
        if (!initTransport())
        {
            fatalRestart("falha ao inicializar transporte");
            return false;
        }
    }

    // Criação das tarefas internas de kernel do FreeRTOS
    bool ok = true;
    ok &= (xTaskCreate(kernelSenderTask, "HiveSender", _config.kernelStackSize,
                       this, HIVE_KERNEL_TASK_PRIORITY, nullptr) == pdPASS);

    if (_config.numNodes > 1)
    {
        ok &= (xTaskCreate(kernelReceiverTask, "HiveReceiver", _config.kernelStackSize,
                           this, HIVE_KERNEL_TASK_PRIORITY, nullptr) == pdPASS);
        ok &= (xTaskCreate(kernelDiscoveryTask, "HiveDiscovery", _config.kernelStackSize,
                           this, HIVE_KERNEL_TASK_PRIORITY, nullptr) == pdPASS);
        ok &= (xTaskCreate(kernelControlTask, "HiveControl", _config.kernelStackSize,
                           this, HIVE_KERNEL_TASK_PRIORITY, nullptr) == pdPASS);
    }

    // Se for o nó 0 (Master), cria a tarefa coordenadora
    if (_config.nodeId == 0)
    {
        ok &= (xTaskCreate(kernelMasterTask, "HiveMaster", _config.kernelStackSize,
                           this, HIVE_MASTER_TASK_PRIORITY, nullptr) == pdPASS);
    }

    if (!ok)
    {
        fatalRestart("falha criando tasks do kernel FreeRTOS");
        return false;
    }

    _initialized = true;

    Serial.println();
    Serial.println("[KERNEL] Kernel ESP-Hive inicializado com sucesso.");
    Serial.printf("[KERNEL] Node ID: %d | Comm size: %d ranks | Transporte: %s\n",
                  _config.nodeId, _config.totalTasks,
                  (_config.commMode == HIVE_MODE_ESPNOW) ? "ESP-NOW" : "WiFi/UDP");

    if (_config.numNodes > 1)
        Serial.println("[KERNEL] Aguardando descoberta dos nós vizinhos...");

    return true;
}

// ============================================================
// TRANSPORTE FÍSICO (ESP-NOW E WI-FI)
// ============================================================
static bool EspNowEnsurePeerLocal(const uint8_t *mac)
{
    if (esp_now_is_peer_exist(mac)) return true;

    esp_now_peer_info_t peer;
    memset(&peer, 0, sizeof(peer));
    memcpy(peer.peer_addr, mac, 6);
    peer.channel = 0;
    peer.ifidx   = WIFI_IF_STA;
    peer.encrypt = false;

    return esp_now_add_peer(&peer) == ESP_OK;
}

bool ESPHiveClass::initTransport()
{
    if (_config.commMode == HIVE_MODE_ESPNOW)
    {
        Serial.println("[COMM] Modo: ESP-NOW");

        s_espnowDataQ      = xQueueCreate(HIVE_ESPNOW_RX_QUEUE_LENGTH, sizeof(EspNowRxPacket));
        s_espnowControlQ   = xQueueCreate(HIVE_ESPNOW_RX_QUEUE_LENGTH, sizeof(EspNowRxPacket));
        s_espnowDiscoveryQ = xQueueCreate(8, sizeof(EspNowRxPacket));

        if (!s_espnowDataQ || !s_espnowControlQ || !s_espnowDiscoveryQ)
            fatalRestart("falha criando filas ESP-NOW");

        WiFi.mode(WIFI_STA);
        WiFi.setSleep(false);
        WiFi.disconnect();

        esp_wifi_set_promiscuous(true);
        esp_wifi_set_channel(_config.espnowChannel, WIFI_SECOND_CHAN_NONE);
        esp_wifi_set_promiscuous(false);

        if (esp_now_init() != ESP_OK)
            fatalRestart("esp_now_init");

        esp_now_register_recv_cb(EspNowRecvCallback);
        esp_now_register_send_cb(EspNowSendCallback);

        if (!EspNowEnsurePeerLocal(kEspNowBroadcastMac))
            fatalRestart("peer de broadcast ESP-NOW");

        Serial.printf("[COMM] MAC deste node: %s | canal %d\n",
                      WiFi.macAddress().c_str(), _config.espnowChannel);

        _nodeDiscovered[_config.nodeId] = true;
        return true;
    }
    else // HIVE_MODE_WIFI
    {
        Serial.println("[COMM] Modo: WiFi/UDP");

        WiFi.mode(WIFI_STA);
        WiFi.setSleep(false);
        WiFi.setAutoReconnect(true);
        WiFi.begin(_config.wifiSSID, _config.wifiPassword);

        Serial.print("[WIFI] Conectando");
        uint32_t startWifi = millis();
        while (WiFi.status() != WL_CONNECTED)
        {
            delay(500);
            Serial.print(".");
            if (millis() - startWifi > 30000)
                fatalRestart("timeout ao conectar no Wi-Fi");
        }

        Serial.println();
        Serial.printf("[WIFI] Conectado! IP: %s\n", WiFi.localIP().toString().c_str());

        _nodeIP[_config.nodeId]         = WiFi.localIP();
        _nodeDiscovered[_config.nodeId] = true;

        if (!s_udpDataRx.begin(HIVE_DATA_PORT))           fatalRestart("bind DATA_PORT");
        if (!s_udpDiscoveryRx.begin(HIVE_DISCOVERY_PORT)) fatalRestart("bind DISCOVERY_PORT");
        if (!s_udpControlRx.begin(HIVE_CONTROL_PORT))     fatalRestart("bind CONTROL_PORT");

        return true;
    }
}

static bool EspNowSendRawLocal(const uint8_t *mac, uint8_t tag, const void *payload, size_t len)
{
    if (len > sizeof(Message)) return false;

    uint8_t frame[1 + sizeof(Message)];
    frame[0] = tag;
    memcpy(frame + 1, payload, len);

    for (int i = 0; i < 5; i++)
    {
        esp_err_t r = esp_now_send(mac, frame, len + 1);
        if (r == ESP_OK) return true;
        if (r == ESP_ERR_ESPNOW_NOT_FOUND) return false;
        vTaskDelay(MsToTicks(2));
    }
    return false;
}

static IPAddress GetBroadcastAddress()
{
    IPAddress ip   = WiFi.localIP();
    IPAddress mask = WiFi.subnetMask();
    return IPAddress(ip[0] | (uint8_t)~mask[0],
                     ip[1] | (uint8_t)~mask[1],
                     ip[2] | (uint8_t)~mask[2],
                     ip[3] | (uint8_t)~mask[3]);
}

void ESPHiveClass::learnNode(int nodeId, const PeerAddr &addr)
{
    if (nodeId < 0 || nodeId >= _config.numNodes || nodeId == _config.nodeId) return;

    if (_config.commMode == HIVE_MODE_ESPNOW)
    {
        bool changed = !_nodeDiscovered[nodeId] || memcmp(_nodeMAC[nodeId], addr.mac, 6) != 0;
        if (!changed) return;

        if (!EspNowEnsurePeerLocal(addr.mac))
        {
            Serial.printf("[DISCOVERY] Falha adicionando peer do node %d\n", nodeId);
            return;
        }

        memcpy(_nodeMAC[nodeId], addr.mac, 6);
        _nodeDiscovered[nodeId] = true;

        Serial.printf("[DISCOVERY] Node %d descoberto! MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",
                      nodeId, addr.mac[0], addr.mac[1], addr.mac[2],
                      addr.mac[3], addr.mac[4], addr.mac[5]);
    }
    else // HIVE_MODE_WIFI
    {
        if (addr.ip == IPAddress(0, 0, 0, 0)) return;

        if (!_nodeDiscovered[nodeId] || _nodeIP[nodeId] != addr.ip)
        {
            _nodeIP[nodeId] = addr.ip;
            _nodeDiscovered[nodeId] = true;
            Serial.printf("[DISCOVERY] Node %d descoberto! IP: %s\n",
                          nodeId, addr.ip.toString().c_str());
        }
    }
}

bool ESPHiveClass::sendControlMsg(int node, const ControlMessage &msg)
{
    if (node < 0 || node >= _config.numNodes || node == _config.nodeId) return false;
    if (!_nodeDiscovered[node]) return false;

    xSemaphoreTake(_controlTxMutex, portMAX_DELAY);
    bool ok = false;

    if (_config.commMode == HIVE_MODE_ESPNOW)
    {
        ok = EspNowSendRawLocal(_nodeMAC[node], HIVE_ESPNOW_TAG_CONTROL, &msg, sizeof(ControlMessage));
    }
    else
    {
        if (s_udpControlTx.beginPacket(_nodeIP[node], HIVE_CONTROL_PORT))
        {
            s_udpControlTx.write((const uint8_t *)&msg, sizeof(ControlMessage));
            ok = (s_udpControlTx.endPacket() == 1);
        }
    }

    xSemaphoreGive(_controlTxMutex);
    return ok;
}

bool ESPHiveClass::sendDataPkt(int node, const Message &msg)
{
    if (node < 0 || node >= _config.numNodes || node == _config.nodeId) return false;
    if (!_nodeDiscovered[node]) return false;

    xSemaphoreTake(_dataTxMutex, portMAX_DELAY);
    bool ok = false;

    if (_config.commMode == HIVE_MODE_ESPNOW)
    {
        ok = EspNowSendRawLocal(_nodeMAC[node], HIVE_ESPNOW_TAG_DATA, &msg, sizeof(Message));
    }
    else
    {
        if (s_udpDataTx.beginPacket(_nodeIP[node], HIVE_DATA_PORT))
        {
            s_udpDataTx.write((const uint8_t *)&msg, sizeof(Message));
            ok = (s_udpDataTx.endPacket() == 1);
        }
    }

    xSemaphoreGive(_dataTxMutex);
    return ok;
}

void ESPHiveClass::sendDiscoveryMsg()
{
    DiscoveryMessage msg;
    memset(&msg, 0, sizeof(msg));
    msg.magic  = HIVE_DISCOVERY_MAGIC;
    msg.nodeId = _config.nodeId;

    xSemaphoreTake(_discoveryTxMutex, portMAX_DELAY);

    if (_config.commMode == HIVE_MODE_ESPNOW)
    {
        EspNowSendRawLocal(kEspNowBroadcastMac, HIVE_ESPNOW_TAG_DISCOVERY, &msg, sizeof(msg));
    }
    else
    {
        IPAddress ip = WiFi.localIP();
        msg.ip[0] = ip[0]; msg.ip[1] = ip[1]; msg.ip[2] = ip[2]; msg.ip[3] = ip[3];

        if (s_udpDiscoveryTx.beginPacket(GetBroadcastAddress(), HIVE_DISCOVERY_PORT))
        {
            s_udpDiscoveryTx.write((const uint8_t *)&msg, sizeof(msg));
            s_udpDiscoveryTx.endPacket();
        }
    }

    xSemaphoreGive(_discoveryTxMutex);
}

bool ESPHiveClass::transportRecv(int channel, void *buf, size_t size, PeerAddr &from, TickType_t wait)
{
    if (_config.commMode == HIVE_MODE_ESPNOW)
    {
        QueueHandle_t q = (channel == CH_DATA)    ? s_espnowDataQ
                        : (channel == CH_CONTROL) ? s_espnowControlQ
                                                  : s_espnowDiscoveryQ;

        EspNowRxPacket p;
        if (xQueueReceive(q, &p, wait) != pdTRUE) return false;
        if (p.len != size) return false;

        memcpy(buf, p.data, size);
        memcpy(from.mac, p.mac, 6);
        return true;
    }
    else
    {
        WiFiUDP &u = (channel == CH_DATA)    ? s_udpDataRx
                   : (channel == CH_CONTROL) ? s_udpControlRx
                                             : s_udpDiscoveryRx;

        int packetSize = u.parsePacket();
        if (packetSize <= 0)
        {
            if (wait > 0) vTaskDelay(MsToTicks(1));
            return false;
        }

        if (packetSize != (int)size)
        {
            while (u.available()) u.read();
            return false;
        }

        from.ip = u.remoteIP();
        int received = u.read((uint8_t *)buf, size);
        return received == (int)size;
    }
}

// ============================================================
// CONTROLE DE TAREFAS E CICLO DE VIDA (MPI & FREERTOS)
// ============================================================
void ESPHiveClass::rankTaskEntry(void *param)
{
    ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

    if (Hive._appTaskFunc != nullptr)
    {
        Hive._appTaskFunc(param);
    }
    else
    {
        ApplicationTask(param);
    }

    vTaskDelete(NULL);
}

bool ESPHiveClass::createLocalRankTask(int rank)
{
    if (!isLocalRank(rank)) return false;

    TaskContext *context = &_taskContexts[rank];
    if (context->taskHandle != nullptr) return true;

    Serial.printf("[KERNEL] Criando rank %d...\n", rank);

    context->rank         = rank;
    context->barrierGen   = 0;
    context->pendingCount = 0;
    context->finalized    = false;

    int queueLen = HIVE_QUEUE_LENGTH;
    if (rank == 0 && queueLen < _config.totalTasks) queueLen = _config.totalTasks;

    context->receiveQueue = xQueueCreate(queueLen, sizeof(Message));
    context->barrierSem   = xSemaphoreCreateBinary();
    context->pending      = (Message *)malloc(sizeof(Message) * HIVE_PENDING_MAX);

    if (!context->receiveQueue || !context->barrierSem || !context->pending)
    {
        Serial.printf("[KERNEL] ERRO alocando recursos do rank %d\n", rank);
        goto fail;
    }

    {
        char taskName[16];
        snprintf(taskName, sizeof(taskName), "APP_%d", rank);

        uint32_t stackSize = (rank < _config.numNodes) ? _config.appStackSize : _config.appStackSizeSmall;

        if (xTaskCreate(rankTaskEntry, taskName, stackSize,
                        context, HIVE_TASK_PRIORITY,
                        &context->taskHandle) != pdPASS)
        {
            Serial.printf("[KERNEL] ERRO ao criar FreeRTOS task para rank %d\n", rank);
            context->taskHandle = nullptr;
            goto fail;
        }
    }

    Serial.printf("[KERNEL] Rank %d criado - aguardando START\n", rank);
    return true;

fail:
    if (context->receiveQueue) { vQueueDelete(context->receiveQueue); context->receiveQueue = nullptr; }
    if (context->barrierSem)   { vSemaphoreDelete(context->barrierSem); context->barrierSem = nullptr; }
    if (context->pending)      { free(context->pending); context->pending = nullptr; }
    return false;
}

void ESPHiveClass::startLocalTasks()
{
    if (_kernelReady) return;
    _kernelReady = true;

    Serial.println();
    Serial.println("[KERNEL] ================================");
    Serial.println("[KERNEL] KERNEL READY - liberando tasks");
    Serial.println("[KERNEL] ================================");

    for (int rank = 0; rank < _config.totalTasks; rank++)
    {
        if (isLocalRank(rank) && _taskContexts[rank].taskHandle != nullptr)
        {
            Serial.printf("[KERNEL] Liberando rank local %d\n", rank);
            xTaskNotifyGive(_taskContexts[rank].taskHandle);
        }
    }
}

void ESPHiveClass::printLocalRanks()
{
    Serial.printf("[KERNEL] Node %d executa os ranks: ", _config.nodeId);
    bool first = true;
    int  count = 0;

    for (int rank = 0; rank < _config.totalTasks; rank++)
    {
        if (!isLocalRank(rank)) continue;
        if (!first) Serial.print(", ");
        Serial.print(rank);
        first = false;
        count++;
    }

    if (count == 0) Serial.print("(nenhum)");
    Serial.printf("  [%d de %d]\n", count, _config.totalTasks);
}

static ControlMessage MakeControlMessage(uint8_t type, uint8_t srcNode, int32_t rank, int32_t generation, int32_t status)
{
    ControlMessage m;
    memset(&m, 0, sizeof(m));
    m.type       = type;
    m.srcNode    = srcNode;
    m.rank       = rank;
    m.generation = generation;
    m.status     = status;
    return m;
}

bool ESPHiveClass::sendCreateTaskCmd(int rank)
{
    int destinationNode = rankToNode(rank);

    if (destinationNode == _config.nodeId)
        return createLocalRankTask(rank);

    for (int attempt = 0; attempt < HIVE_CONTROL_MAX_RETRIES; attempt++)
    {
        if (!_nodeDiscovered[destinationNode])
        {
            vTaskDelay(MsToTicks(500));
            continue;
        }

        while (xSemaphoreTake(_createAckSemaphore, 0) == pdTRUE) {}
        _lastCreateAckRank   = -1;
        _lastCreateAckStatus = 0;

        ControlMessage msg = MakeControlMessage(CONTROL_CREATE_TASK, _config.nodeId, rank, 0, 0);
        if (!sendControlMsg(destinationNode, msg))
        {
            vTaskDelay(MsToTicks(100));
            continue;
        }

        TickType_t start   = xTaskGetTickCount();
        TickType_t timeout = MsToTicks(HIVE_CONTROL_ACK_TIMEOUT_MS);

        while (true)
        {
            TickType_t elapsed = xTaskGetTickCount() - start;
            if (elapsed >= timeout) break;

            if (xSemaphoreTake(_createAckSemaphore, timeout - elapsed) == pdTRUE)
            {
                if (_lastCreateAckRank == rank)
                {
                    if (_lastCreateAckStatus == 1)
                    {
                        Serial.printf("[MASTER] Rank %d confirmado pelo node %d\n", rank, destinationNode);
                        return true;
                    }
                    return false;
                }
            }
        }

        Serial.printf("[MASTER] Timeout CREATE rank %d (tentativa %d)\n", rank, attempt + 1);
    }
    return false;
}

bool ESPHiveClass::sendStartCmd(int destinationNode)
{
    for (int attempt = 0; attempt < HIVE_CONTROL_MAX_RETRIES; attempt++)
    {
        if (!_nodeDiscovered[destinationNode])
        {
            vTaskDelay(MsToTicks(500));
            continue;
        }

        while (xSemaphoreTake(_startAckSemaphore, 0) == pdTRUE) {}
        _lastStartAckStatus = 0;

        Serial.printf("[MASTER] Enviando START ao node %d\n", destinationNode);
        ControlMessage msg = MakeControlMessage(CONTROL_START, _config.nodeId, -1, 0, 0);

        if (!sendControlMsg(destinationNode, msg))
        {
            vTaskDelay(MsToTicks(100));
            continue;
        }

        if (xSemaphoreTake(_startAckSemaphore, MsToTicks(HIVE_CONTROL_ACK_TIMEOUT_MS)) == pdTRUE)
            return (_lastStartAckStatus == 1);
    }
    return false;
}

// ============================================================
// BARREIRA DISTRIBUÍDA
// ============================================================
void ESPHiveClass::sendBarrierRelease(int node, int32_t generation)
{
    if (node < 0 || node >= _config.numNodes || node == _config.nodeId) return;
    if (!_nodeDiscovered[node]) return;

    ControlMessage rel = MakeControlMessage(CONTROL_BARRIER_RELEASE, _config.nodeId, -1, generation, 1);
    sendControlMsg(node, rel);
}

void ESPHiveClass::barrierReleaseLocal(int32_t generation)
{
    xSemaphoreTake(_barrierMutex, portMAX_DELAY);

    for (int i = 0; i < _config.totalTasks; i++)
    {
        if (isLocalRank(i) && _barrierWaitGen[i] == generation)
        {
            _barrierWaitGen[i] = -1;
            if (_taskContexts[i].barrierSem != nullptr)
                xSemaphoreGive(_taskContexts[i].barrierSem);
        }
    }

    xSemaphoreGive(_barrierMutex);
}

void ESPHiveClass::barrierHandleArrival(int rank, int32_t generation)
{
    if (rank < 0 || rank >= _config.totalTasks) return;

    bool    completed  = false;
    bool    stale      = false;
    int32_t releaseGen = generation;

    xSemaphoreTake(_barrierMutex, portMAX_DELAY);

    if (generation < _coordGeneration)
    {
        stale = true;
    }
    else if (generation == _coordGeneration)
    {
        _barrierArrivedGen[rank] = generation;

        int count = 0;
        for (int i = 0; i < _config.totalTasks; i++)
            if (_barrierArrivedGen[i] == generation) count++;

        if (_config.verbose)
        {
            Serial.printf("[BARRIER] Rank %d chegou, geracao %ld (%d/%d)\n",
                          rank, (long)generation, count, _config.totalTasks);
        }

        if (count == _config.totalTasks)
        {
            completed = true;
            releaseGen = generation;
            _coordGeneration = generation + 1;
        }
    }

    xSemaphoreGive(_barrierMutex);

    if (completed)
    {
        if (_config.verbose)
            Serial.printf("[BARRIER] Geracao %ld completa - liberando cluster\n", (long)releaseGen);

        barrierReleaseLocal(releaseGen);

        for (int n = 0; n < _config.numNodes; n++)
            if (n != _config.nodeId) sendBarrierRelease(n, releaseGen);
    }
    else if (stale && !isLocalRank(rank))
    {
        sendBarrierRelease(rankToNode(rank), generation);
    }
}

// ============================================================
// TAREFAS INTERNAS DE KERNEL (FREERTOS)
// ============================================================
void ESPHiveClass::kernelMasterTask(void *param)
{
    ESPHiveClass *hive = (ESPHiveClass *)param;

    if (hive->_config.numNodes > 1)
    {
        Serial.println("\n[MASTER] Aguardando discovery dos nós workers...");
        for (int n = 0; n < hive->_config.numNodes; n++)
        {
            while (!hive->_nodeDiscovered[n])
                vTaskDelay(MsToTicks(500));
        }
        Serial.println("[MASTER] Todos os nós foram descobertos na rede.");
    }

    Serial.println("[MASTER] Iniciando criação das Application Tasks...");

    for (int rank = 0; rank < hive->_config.totalTasks; rank++)
    {
        Serial.printf("[MASTER] Criando rank %d...\n", rank);
        if (!hive->sendCreateTaskCmd(rank))
        {
            Serial.printf("[MASTER] ERRO fatal criando rank %d\n", rank);
            vTaskDelete(NULL);
            return;
        }
        vTaskDelay(MsToTicks(30));
    }

    Serial.println("[MASTER] Todas as tasks foram criadas em seus respectivos nós.");

    for (int n = 0; n < hive->_config.numNodes; n++)
    {
        if (n == hive->_config.nodeId) continue;
        if (!hive->sendStartCmd(n))
        {
            Serial.printf("[MASTER] ERRO fatal no START do node %d\n", n);
            vTaskDelete(NULL);
            return;
        }
        Serial.printf("[MASTER] Node %d confirmou START\n", n);
    }

    hive->startLocalTasks();
    Serial.println("[MASTER] SISTEMA DISTRIBUÍDO PRONTO E EM EXECUÇÃO.");

    vTaskDelete(NULL);
}

void ESPHiveClass::kernelSenderTask(void *param)
{
    ESPHiveClass *hive = (ESPHiveClass *)param;
    Message msg;

    while (true)
    {
        if (xQueueReceive(hive->_senderQueue, &msg, portMAX_DELAY) != pdTRUE)
            continue;

        int destinationNode = hive->rankToNode(msg.dstRank);

        // Entrega Local
        if (destinationNode == hive->_config.nodeId)
        {
            TaskContext *dst = hive->getTaskContext(msg.dstRank);
            if (dst == nullptr || dst->receiveQueue == nullptr)
            {
                Serial.printf("[KERNEL] Falha na entrega local %d -> %d: contexto invalido\n",
                              (int)msg.srcRank, (int)msg.dstRank);
                continue;
            }

            bool delivered = false;
            for (int attempt = 0; attempt < HIVE_DATA_MAX_RETRIES && !delivered; attempt++)
            {
                if (xQueueSend(dst->receiveQueue, &msg, MsToTicks(1000)) == pdTRUE)
                {
                    delivered = true;
                }
                else
                {
                    Serial.printf("[KERNEL] Fila local do rank %d cheia - tentativa %d/%d\n",
                                  (int)msg.dstRank, attempt + 1, HIVE_DATA_MAX_RETRIES);
                }
            }

            if (!delivered)
            {
                Serial.printf("[KERNEL] ERRO: falha definitiva na entrega local %d -> %d\n",
                              (int)msg.srcRank, (int)msg.dstRank);
            }
            continue;
        }

        // Entrega Remota
        LinkStats &ls = hive->_linkStats[destinationNode];
        ls.msgs++;
        bool acked = false;

        for (int attempt = 0; attempt < HIVE_DATA_MAX_RETRIES && !acked; attempt++)
        {
            if (!hive->_nodeDiscovered[destinationNode])
            {
                vTaskDelay(MsToTicks(100));
                continue;
            }

            while (xSemaphoreTake(hive->_dataAckSemaphore, 0) == pdTRUE) {}

            if (!hive->sendDataPkt(destinationNode, msg))
            {
                ls.txSendFail++;
                vTaskDelay(MsToTicks(10));
                continue;
            }

            ls.txAttempts++;

            TickType_t start   = xTaskGetTickCount();
            TickType_t timeout = MsToTicks(HIVE_DATA_ACK_TIMEOUT_MS);

            while (!acked)
            {
                TickType_t elapsed = xTaskGetTickCount() - start;
                if (elapsed >= timeout) break;

                if (xSemaphoreTake(hive->_dataAckSemaphore, timeout - elapsed) == pdTRUE)
                {
                    if (hive->_lastAckSeq == msg.sequence)
                    {
                        acked = true;
                        ls.ackOk++;
                    }
                }
            }
        }

        if (!acked)
        {
            ls.msgFailed++;
            Serial.printf("[KERNEL] ERRO: sem ACK de %d -> %d (seq %lu)\n",
                          (int)msg.srcRank, (int)msg.dstRank, (unsigned long)msg.sequence);
        }
    }
}

void ESPHiveClass::kernelReceiverTask(void *param)
{
    ESPHiveClass *hive = (ESPHiveClass *)param;

    while (true)
    {
        Message  msg;
        PeerAddr from;

        if (!hive->transportRecv(CH_DATA, &msg, sizeof(Message), from, MsToTicks(10)))
            continue;

        // ACK de dados
        if (msg.type == MSG_DATA_ACK)
        {
            int ackNode = hive->rankToNode(msg.srcRank);
            if (ackNode >= 0 && ackNode < hive->_config.numNodes && ackNode != hive->_config.nodeId)
                hive->_linkStats[ackNode].ackRx++;

            hive->_lastAckSeq = msg.sequence;
            xSemaphoreGive(hive->_dataAckSemaphore);
            continue;
        }

        // Mensagem de dados da aplicação
        if (msg.type != MSG_APPLICATION) continue;
        if (msg.srcRank < 0 || msg.srcRank >= hive->_config.totalTasks) continue;
        if (!hive->isLocalRank(msg.dstRank)) continue;
        if (msg.length > HIVE_MESSAGE_PAYLOAD) continue;

        int srcNode = hive->rankToNode(msg.srcRank);
        if (srcNode == hive->_config.nodeId) continue;

        hive->learnNode(srcNode, from);

        LinkStats &ls = hive->_linkStats[srcNode];
        ls.rxData++;

        TaskContext *dst = hive->getTaskContext(msg.dstRank);
        if (dst == nullptr || dst->receiveQueue == nullptr) continue;

        bool duplicate = (hive->_lastDeliveredSeq[srcNode] == (int64_t)msg.sequence);
        bool delivered = duplicate;

        if (duplicate)
        {
            ls.rxDup++;
        }
        else
        {
            delivered = (xQueueSend(dst->receiveQueue, &msg, MsToTicks(50)) == pdTRUE);
            if (delivered)
            {
                hive->_lastDeliveredSeq[srcNode] = (int64_t)msg.sequence;
                ls.rxUnique++;
            }
            else
            {
                ls.rxQueueFull++;
                Serial.printf("[KERNEL] Fila do rank %d cheia - pacote descartado\n", (int)msg.dstRank);
            }
        }

        if (delivered)
        {
            Message ack;
            memset(&ack, 0, sizeof(ack));
            ack.type     = MSG_DATA_ACK;
            ack.srcRank  = msg.dstRank;
            ack.dstRank  = msg.srcRank;
            ack.sequence = msg.sequence;

            hive->sendDataPkt(srcNode, ack);
            ls.ackTx++;
        }
    }
}

void ESPHiveClass::kernelDiscoveryTask(void *param)
{
    ESPHiveClass *hive = (ESPHiveClass *)param;
    Serial.println("[DISCOVERY] Task iniciada");

    uint32_t lastSend = 0;
    bool reportedAll  = false;

    while (true)
    {
        bool allFound = true;
        for (int i = 0; i < hive->_config.numNodes; i++)
            if (!hive->_nodeDiscovered[i]) { allFound = false; break; }

        uint32_t interval = allFound ? 5000 : 1000;
        uint32_t now = millis();

        if (now - lastSend >= interval)
        {
            hive->sendDiscoveryMsg();
            lastSend = now;
        }

        DiscoveryMessage msg;
        PeerAddr from;

        if (hive->transportRecv(CH_DISCOVERY, &msg, sizeof(msg), from, 0))
        {
            if (msg.magic == HIVE_DISCOVERY_MAGIC)
                hive->learnNode(msg.nodeId, from);
        }

        if (allFound && !reportedAll)
        {
            reportedAll = true;
            Serial.println("[DISCOVERY] Todos os nós do cluster foram descobertos!");
        }

        vTaskDelay(MsToTicks(10));
    }
}

void ESPHiveClass::kernelControlTask(void *param)
{
    ESPHiveClass *hive = (ESPHiveClass *)param;
    Serial.printf("[CONTROL] Task iniciada no nó %d\n", hive->_config.nodeId);

    while (true)
    {
        ControlMessage msg;
        PeerAddr from;

        if (!hive->transportRecv(CH_CONTROL, &msg, sizeof(ControlMessage), from, MsToTicks(10)))
            continue;

        hive->learnNode(msg.srcNode, from);

        switch (msg.type)
        {
        case CONTROL_CREATE_TASK:
        {
            Serial.printf("[CONTROL] CREATE rank %d recebido\n", (int)msg.rank);
            bool ok = hive->createLocalRankTask(msg.rank);
            ControlMessage ack = MakeControlMessage(CONTROL_CREATE_ACK, hive->_config.nodeId, msg.rank, 0, ok ? 1 : 0);
            hive->sendControlMsg(msg.srcNode, ack);
            break;
        }

        case CONTROL_CREATE_ACK:
            hive->_lastCreateAckRank   = msg.rank;
            hive->_lastCreateAckStatus = msg.status;
            xSemaphoreGive(hive->_createAckSemaphore);
            break;

        case CONTROL_START:
        {
            Serial.println("[CONTROL] START recebido");
            hive->startLocalTasks();
            ControlMessage ack = MakeControlMessage(CONTROL_START_ACK, hive->_config.nodeId, -1, 0, 1);
            hive->sendControlMsg(msg.srcNode, ack);
            break;
        }

        case CONTROL_START_ACK:
            hive->_lastStartAckStatus = msg.status;
            xSemaphoreGive(hive->_startAckSemaphore);
            break;

        case CONTROL_BARRIER:
            if (hive->_config.nodeId == 0)
                hive->barrierHandleArrival(msg.rank, msg.generation);
            break;

        case CONTROL_BARRIER_RELEASE:
            if (hive->_config.nodeId != 0)
            {
                if (hive->_config.verbose)
                    Serial.printf("[BARRIER] RELEASE geracao %ld recebido\n", (long)msg.generation);
                hive->barrierReleaseLocal(msg.generation);
            }
            break;

        case CONTROL_STATS:
        {
            int n = msg.srcNode;
            if (n >= 0 && n < hive->_config.numNodes && n != hive->_config.nodeId)
            {
                if (msg.generation == 0)
                {
                    hive->_peerSnap[n].txAttempts = msg.stat[0];
                    hive->_peerSnap[n].rxData     = msg.stat[1];
                    hive->_peerSnap[n].ackTx      = msg.stat[2];
                    hive->_peerSnap[n].ackRx      = msg.stat[3];
                    hive->_peerSnap[n].valid      = true;
                }
                else
                {
                    hive->_peerRun[n].valid      = false;
                    hive->_peerRun[n].txAttempts = msg.stat[0];
                    hive->_peerRun[n].rxData     = msg.stat[1];
                    hive->_peerRun[n].ackTx      = msg.stat[2];
                    hive->_peerRun[n].ackRx      = msg.stat[3];
                    hive->_peerRunGen[n]         = msg.generation;
                    hive->_peerRun[n].valid      = true;
                }
            }
            break;
        }

        default:
            break;
        }
    }
}

// ============================================================
// SUPERVISÃO NO LOOP()
// ============================================================
void ESPHiveClass::update()
{
    if (!_initialized) return;

    // Reconexão Wi-Fi se necessário
    if (_config.commMode == HIVE_MODE_WIFI && _config.numNodes > 1)
    {
        if (WiFi.status() != WL_CONNECTED)
        {
            Serial.println("[WIFI] Desconectado - reconectando...");
            WiFi.reconnect();
            vTaskDelay(MsToTicks(2000));
            return;
        }
        if (_nodeIP[_config.nodeId] != WiFi.localIP())
            _nodeIP[_config.nodeId] = WiFi.localIP();
    }

    // Impressão periódica de estatísticas
    if (_config.statsPrintPeriodMs > 0 && _config.numNodes > 1)
    {
        if (millis() - _lastLoopStats >= _config.statsPrintPeriodMs)
        {
            _lastLoopStats = millis();
            printLocalStats();
        }
    }

    // Monitoramento de stack
    if (_config.debugStack)
    {
        if (millis() - _lastLoopStackCheck > 10000)
        {
            _lastLoopStackCheck = millis();
            for (int i = 0; i < _config.totalTasks; i++)
            {
                if (_taskContexts[i].taskHandle != nullptr)
                {
                    Serial.printf("[STACK] APP_%d livre (min): %u bytes\n", i,
                                  (unsigned)uxTaskGetStackHighWaterMark(_taskContexts[i].taskHandle));
                }
            }
        }
    }

    vTaskDelay(MsToTicks(50));
}

// ============================================================
// MÉTODOS PÚBLICOS DE SUPORTE E ESTATÍSTICAS
// ============================================================
void ESPHiveClass::setAppTask(HiveAppTaskFunction task)
{
    _appTaskFunc = task;
}

bool ESPHiveClass::isReady() const
{
    return _kernelReady;
}

int ESPHiveClass::getNodeId() const
{
    return _config.nodeId;
}

int ESPHiveClass::getNumNodes() const
{
    return _config.numNodes;
}

int ESPHiveClass::getTotalTasks() const
{
    return _config.totalTasks;
}

bool ESPHiveClass::isMaster() const
{
    return (_config.nodeId == 0);
}

bool ESPHiveClass::isLocalRank(int rank) const
{
    if (rank < 0 || rank >= _config.totalTasks) return false;
    return (rank % _config.numNodes) == _config.nodeId;
}

int ESPHiveClass::rankToNode(int rank) const
{
    return rank % _config.numNodes;
}

TaskContext* ESPHiveClass::getTaskContext(int rank)
{
    if (rank < 0 || rank >= _config.totalTasks || _taskContexts == nullptr) return nullptr;
    return &_taskContexts[rank];
}

TaskContext* ESPHiveClass::getCurrentTaskContext()
{
    if (_taskContexts == nullptr) return nullptr;

    TaskHandle_t h = xTaskGetCurrentTaskHandle();
    for (int i = 0; i < _config.totalTasks; i++)
    {
        if (_taskContexts[i].taskHandle == h)
            return &_taskContexts[i];
    }
    return nullptr;
}

LinkStats* ESPHiveClass::getLinkStats(int node)
{
    if (node < 0 || node >= _config.numNodes || _linkStats == nullptr) return nullptr;
    return &_linkStats[node];
}

void ESPHiveClass::resetStats()
{
    if (_linkStats == nullptr) return;

    for (int n = 0; n < _config.numNodes; n++)
    {
        memset((void *)&_linkStats[n], 0, sizeof(LinkStats));
        if (_peerSnap) _peerSnap[n].valid = false;
        if (_peerRun)  _peerRun[n].valid  = false;
        if (_peerRunGen) _peerRunGen[n]   = -1;
    }

    _espnowRxDrop = 0;
    _macTxOk      = 0;
    _macTxFail    = 0;
}

static float LossPercentage(uint32_t sent, uint32_t received)
{
    if (sent == 0) return -1.0f;
    if (received >= sent) return 0.0f;
    return 100.0f * (float)(sent - received) / (float)sent;
}

static void PrintFormattedPercentage(const char *label, float pct)
{
    if (pct < 0) Serial.printf("[STATS]    %-34s n/d\n", label);
    else         Serial.printf("[STATS]    %-34s %.2f%%\n", label, pct);
}

void ESPHiveClass::printLocalStats()
{
    Serial.println("---------------- LINK STATS (local) ----------------");

    for (int n = 0; n < _config.numNodes; n++)
    {
        if (n == _config.nodeId) continue;

        LinkStats &s = _linkStats[n];

        Serial.printf("[STATS] -> node %d | msgs=%lu tentativas=%lu acks=%lu falhaTX=%lu perdidasDeVez=%lu\n",
                      n, (unsigned long)s.msgs, (unsigned long)s.txAttempts,
                      (unsigned long)s.ackOk, (unsigned long)s.txSendFail, (unsigned long)s.msgFailed);

        Serial.printf("[STATS] <- node %d | rxData=%lu unico=%lu dup=%lu filaCheia=%lu ackTx=%lu ackRx=%lu\n",
                      n, (unsigned long)s.rxData, (unsigned long)s.rxUnique,
                      (unsigned long)s.rxDup, (unsigned long)s.rxQueueFull,
                      (unsigned long)s.ackTx, (unsigned long)s.ackRx);

        PrintFormattedPercentage("perda ida+volta (por tentativa)", LossPercentage(s.txAttempts, s.ackOk));

        float retx = (s.txAttempts == 0) ? -1.0f
                   : 100.0f * (float)(s.txAttempts - s.msgs) / (float)s.txAttempts;
        PrintFormattedPercentage("retransmissoes / tentativas", retx);
        PrintFormattedPercentage("mensagens perdidas de vez", LossPercentage(s.msgs, s.msgs - s.msgFailed));

        if (s.rxData > 0)
            PrintFormattedPercentage("duplicatas / rxData (ACK perdido)", 100.0f * (float)s.rxDup / (float)s.rxData);
    }

    if (_config.commMode == HIVE_MODE_ESPNOW)
    {
        Serial.printf("[STATS] ESP-NOW MAC unicast ok=%lu fail=%lu | rxQueueDrop=%lu\n",
                      (unsigned long)_macTxOk, (unsigned long)_macTxFail, (unsigned long)_espnowRxDrop);
        PrintFormattedPercentage("falha MAC (apos retries do HW)", LossPercentage(_macTxOk + _macTxFail, _macTxOk));
    }
}

void ESPHiveClass::exchangeAndPrintStats(uint32_t waitMs)
{
    if (_config.numNodes == 1)
    {
        Serial.println("[STATS] Modo Standalone: sem rede, nada a medir");
        return;
    }

    Serial.println("[STATS] Trocando snapshots acumulados com os nós vizinhos...");

    uint32_t t0 = millis();
    while (millis() - t0 < waitMs)
    {
        for (int n = 0; n < _config.numNodes; n++)
        {
            if (n == _config.nodeId) continue;
            LinkStats &s = _linkStats[n];
            ControlMessage m = MakeControlMessage(CONTROL_STATS, _config.nodeId, -1, 0, 0);
            m.stat[0] = s.txAttempts;
            m.stat[1] = s.rxData;
            m.stat[2] = s.ackTx;
            m.stat[3] = s.ackRx;
            sendControlMsg(n, m);
        }
        vTaskDelay(MsToTicks(500));
    }

    printLocalStats();

    Serial.println("------- LINK STATS ACUMULADO DO BENCHMARK (ida/volta) -------");
    Serial.println("[CSV] node,peer,msgs,tent,ackOk,msgFalha,rxData,dup,filaCheia,perdaDados_pct,perdaAck_pct,perdaRT_pct");

    for (int n = 0; n < _config.numNodes; n++)
    {
        if (n == _config.nodeId) continue;

        LinkStats &s    = _linkStats[n];
        PeerSnapshot &p = _peerSnap[n];

        Serial.printf("[STATS] ===== link node %d <-> node %d =====\n", _config.nodeId, n);

        float lossRT   = LossPercentage(s.txAttempts, s.ackOk);
        float lossData = -1, lossAck = -1;

        if (!p.valid)
        {
            Serial.printf("[STATS]    snapshot do node %d NAO recebido (so a medicao local vale)\n", n);
        }
        else
        {
            lossData = LossPercentage(s.txAttempts, p.rxData);
            lossAck  = LossPercentage(p.ackTx, s.ackRx);

            Serial.printf("[STATS]  %d -> %d  (dados %d->%d, ACK %d->%d)\n",
                          _config.nodeId, n, _config.nodeId, n, n, _config.nodeId);
            PrintFormattedPercentage("perda de DADOS (ida)", lossData);
            PrintFormattedPercentage("perda de ACKs (volta)", lossAck);

            Serial.printf("[STATS]  %d -> %d  (dados %d->%d, ACK %d->%d)\n",
                          n, _config.nodeId, n, _config.nodeId, _config.nodeId, n);
            PrintFormattedPercentage("perda de DADOS (ida)", LossPercentage(p.txAttempts, s.rxData));
            PrintFormattedPercentage("perda de ACKs (volta)", LossPercentage(s.ackTx, p.ackRx));
        }

        Serial.printf("[CSV] %d,%d,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%.3f,%.3f,%.3f\n",
                      _config.nodeId, n,
                      (unsigned long)s.msgs, (unsigned long)s.txAttempts,
                      (unsigned long)s.ackOk, (unsigned long)s.msgFailed,
                      (unsigned long)s.rxData, (unsigned long)s.rxDup,
                      (unsigned long)s.rxQueueFull,
                      lossData, lossAck, lossRT);
    }
}

void ESPHiveClass::calibrateTimer()
{
    const int K = 4000;
    int64_t acc = 0;
    for (int i = 0; i < K; i++)
    {
        int64_t a = esp_timer_get_time();
        int64_t b = esp_timer_get_time();
        acc += (b - a);
    }
    _timerOvhUs = (double)acc / (double)K;
}

double ESPHiveClass::getTimerOverheadUs() const
{
    return _timerOvhUs;
}

void ESPHiveClass::fatalRestart(const char *why)
{
    Serial.printf("[KERNEL] FATAL: %s - reiniciando ESP32 em 2 segundos...\n", why);
    delay(2000);
    ESP.restart();
    while (true) { delay(1000); }
}

// ============================================================
// IMPLEMENTAÇÃO DAS FUNÇÕES PADRÃO MPI (C INTERFACE)
// ============================================================
int MPI_Comm_rank(void)
{
    TaskContext *c = Hive.getCurrentTaskContext();
    return c ? c->rank : -1;
}

int MPI_Comm_size(void)
{
    return Hive.getTotalTasks();
}

double MPI_Wtime(void)
{
    return (double)esp_timer_get_time() / 1000000.0;
}

double MPI_Wtime_micro(void)
{
    return (double)esp_timer_get_time();
}

static bool CheckRankNotFinalized(TaskContext *context, const char *fname)
{
    if (context == nullptr) return false;
    if (context->finalized)
    {
        Serial.printf("[MPI] ERRO: %s chamado APÓS MPI_Finalize() no rank %d\n", fname, context->rank);
        return false;
    }
    return true;
}

bool MPI_Send(const void *buffer, int count, int destinationRank)
{
    TaskContext *context = Hive.getCurrentTaskContext();
    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Send: contexto inexistente");
        return false;
    }

    if (!CheckRankNotFinalized(context, "MPI_Send")) return false;

    if (destinationRank < 0 || destinationRank >= Hive.getTotalTasks())
    {
        Serial.printf("[MPI] MPI_Send: destino invalido %d\n", destinationRank);
        return false;
    }

    if (count < 0 || count > HIVE_MESSAGE_PAYLOAD || (count > 0 && buffer == nullptr))
    {
        Serial.printf("[MPI] MPI_Send: tamanho de buffer invalido %d (max: %d)\n", count, HIVE_MESSAGE_PAYLOAD);
        return false;
    }

    Message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type     = MSG_APPLICATION;
    msg.srcRank  = context->rank;
    msg.dstRank  = destinationRank;

    portENTER_CRITICAL(&Hive._sequenceMux);
    msg.sequence = Hive._nextSequence++;
    portEXIT_CRITICAL(&Hive._sequenceMux);

    msg.length   = count;
    if (count > 0) memcpy(msg.payload, buffer, count);

    if (xQueueSend(Hive._senderQueue, &msg, portMAX_DELAY) != pdTRUE)
    {
        Serial.println("[MPI] Erro ao enfileirar no senderQueue");
        return false;
    }

    return true;
}

static int CopyOutMessage(TaskContext *context, const Message &msg, void *buffer, int maxCount)
{
    int copySize = msg.length;
    if (copySize > maxCount) copySize = maxCount;
    if (copySize > HIVE_MESSAGE_PAYLOAD) copySize = HIVE_MESSAGE_PAYLOAD;
    if (copySize > 0 && buffer != nullptr)
        memcpy(buffer, msg.payload, copySize);
    return copySize;
}

int MPI_Recv(void *buffer, int maxCount, int sourceRank)
{
    TaskContext *context = Hive.getCurrentTaskContext();
    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Recv: contexto inexistente");
        return -1;
    }

    if (!CheckRankNotFinalized(context, "MPI_Recv")) return -1;

    // 1. Verifica se já temos mensagem armazenada no buffer de pendentes
    for (int i = 0; i < context->pendingCount; i++)
    {
        if (sourceRank < 0 || context->pending[i].srcRank == sourceRank)
        {
            Message msg = context->pending[i];
            for (int j = i + 1; j < context->pendingCount; j++)
                context->pending[j - 1] = context->pending[j];

            context->pendingCount--;
            return CopyOutMessage(context, msg, buffer, maxCount);
        }
    }

    // 2. Aguarda mensagem na fila da task
    Message msg;
    while (true)
    {
        if (xQueueReceive(context->receiveQueue, &msg, portMAX_DELAY) != pdTRUE)
            continue;

        if (sourceRank < 0 || msg.srcRank == sourceRank)
            return CopyOutMessage(context, msg, buffer, maxCount);

        if (context->pendingCount < HIVE_PENDING_MAX)
        {
            context->pending[context->pendingCount++] = msg;
        }
        else
        {
            Serial.printf("[MPI] Rank %d: buffer pendente cheio, descartando msg de %d\n",
                          context->rank, (int)msg.srcRank);
        }
    }
}

void MPI_Barrier(void)
{
    TaskContext *context = Hive.getCurrentTaskContext();
    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Barrier: contexto inexistente");
        return;
    }

    if (!CheckRankNotFinalized(context, "MPI_Barrier")) return;

    int     rank       = context->rank;
    int32_t generation = context->barrierGen++;

    xSemaphoreTake(Hive._barrierMutex, portMAX_DELAY);
    Hive._barrierWaitGen[rank] = generation;
    xSemaphoreGive(Hive._barrierMutex);

    if (Hive.getNodeId() == 0)
    {
        Hive.barrierHandleArrival(rank, generation);
        xSemaphoreTake(context->barrierSem, portMAX_DELAY);
    }
    else
    {
        while (true)
        {
            if (Hive._nodeDiscovered[0])
            {
                ControlMessage m = MakeControlMessage(CONTROL_BARRIER, Hive.getNodeId(), rank, generation, 0);
                Hive.sendControlMsg(0, m);
            }

            if (xSemaphoreTake(context->barrierSem, MsToTicks(HIVE_BARRIER_RESEND_MS)) == pdTRUE)
                break;
        }
    }

    if (Hive.getConfig().verbose)
        Serial.printf("[BARRIER] Rank %d liberado da geracao %ld\n", rank, (long)generation);
}

bool MPI_Bcast(void *buffer, int count, int rootRank)
{
    TaskContext *context = Hive.getCurrentTaskContext();
    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Bcast: contexto inexistente");
        return false;
    }

    if (!CheckRankNotFinalized(context, "MPI_Bcast")) return false;

    if (rootRank < 0 || rootRank >= Hive.getTotalTasks())
    {
        Serial.printf("[MPI] MPI_Bcast: raiz invalida %d\n", rootRank);
        return false;
    }

    if (count < 0 || count > HIVE_MESSAGE_PAYLOAD || (count > 0 && buffer == nullptr))
    {
        Serial.printf("[MPI] MPI_Bcast: tamanho invalido %d\n", count);
        return false;
    }

    int rank = context->rank;
    if (rank == rootRank)
    {
        bool allOk = true;
        for (int dst = 0; dst < Hive.getTotalTasks(); dst++)
        {
            if (dst == rootRank) continue;
            if (!MPI_Send(buffer, count, dst))
            {
                Serial.printf("[MPI] MPI_Bcast: falha enviando %d -> %d\n", rootRank, dst);
                allOk = false;
            }
        }
        return allOk;
    }

    int received = MPI_Recv(buffer, count, rootRank);
    return received >= 0;
}

bool MPI_Finalize(void)
{
    TaskContext *context = Hive.getCurrentTaskContext();
    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Finalize: contexto inexistente");
        return false;
    }

    if (context->finalized)
    {
        Serial.printf("[MPI] MPI_Finalize: rank %d ja finalizado\n", context->rank);
        return true;
    }

    context->finalized = true;
    Serial.printf("[MPI] Rank %d: MPI_Finalize() concluído com sucesso.\n", context->rank);
    return true;
}

bool MPI_Finalized(void)
{
    TaskContext *context = Hive.getCurrentTaskContext();
    return (context != nullptr) && context->finalized;
}
