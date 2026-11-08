#ifndef ESP_HIVE_H
#define ESP_HIVE_H

#include <Arduino.h>
#include "ESPHive_Config.h"
#include "ESPHive_Types.h"
#include "ESPHive_MPI.h"

typedef void (*HiveAppTaskFunction)(void *parameter);

/**
 * @brief Gerenciador principal do cluster distribuído ESP-Hive.
 */
class ESPHiveClass
{
public:
    ESPHiveClass();

    /**
     * @brief Inicializa o nó no cluster ESP-Hive com uma estrutura de configuração.
     * @param cfg Estrutura com parâmetros de rede, nós, tarefas e stacks.
     * @return true se o kernel foi inicializado com sucesso.
     */
    bool begin(const HiveConfig &cfg);

    /**
     * @brief Inicializa o nó no cluster ESP-Hive de forma simplificada.
     * @param nodeId ID deste nó (0 = Coordenador/Master, 1..N-1 = Workers).
     * @param numNodes Número total de nós físicos no cluster.
     * @param totalTasks Número total de tarefas/ranks distribuídos no cluster.
     * @param commMode Modo de rádio: HIVE_MODE_ESPNOW (padrão) ou HIVE_MODE_WIFI.
     * @return true se o kernel foi inicializado com sucesso.
     */
    bool begin(int nodeId = 0, int numNodes = 1, int totalTasks = 1,
               HiveCommMode commMode = HIVE_MODE_ESPNOW);

    /**
     * @brief Rotina periódica de supervisão do cluster.
     * Deve ser chamada dentro de void loop() no sketch do Arduino.
     */
    void update();

    /**
     * @brief Registra a função de execução da aplicação para os ranks locais.
     * Se não for chamada, a biblioteca procura por void ApplicationTask(void *parameter).
     * @param task Ponteiro para a função da aplicação.
     */
    void setAppTask(HiveAppTaskFunction task);

    /**
     * @brief Verifica se o kernel e todas as tarefas distribuídas estão sincronizados e prontos.
     */
    bool isReady() const;

    /**
     * @brief Retorna o identificador físico do nó local.
     */
    int getNodeId() const;

    /**
     * @brief Retorna o total de placas configuradas no cluster.
     */
    int getNumNodes() const;

    /**
     * @brief Retorna o total de ranks MPI do cluster.
     */
    int getTotalTasks() const;

    /**
     * @brief Retorna se o nó atual é o Master (Node 0).
     */
    bool isMaster() const;

    /**
     * @brief Verifica se um determinado rank MPI executa fisicamente neste nó.
     */
    bool isLocalRank(int rank) const;

    /**
     * @brief Identifica qual nó físico é responsável por executar um rank específico.
     */
    int rankToNode(int rank) const;

    /**
     * @brief Zera todos os contadores de pacotes, tentativas, perdas e retransmissões.
     */
    void resetStats();

    /**
     * @brief Imprime no Serial as estatísticas locais de comunicação de cada link.
     */
    void printLocalStats();

    /**
     * @brief Troca snapshots de estatísticas entre todos os nós e imprime relatório consolidado.
     * @param waitMs Tempo em milissegundos dedicado à troca dos pacotes de estatísticas.
     */
    void exchangeAndPrintStats(uint32_t waitMs = 4000);

    /**
     * @brief Obtém o ponteiro para a estrutura de estatísticas de um nó parceiro.
     */
    LinkStats* getLinkStats(int node);

    /**
     * @brief Calibra o overhead de leitura do temporizador esp_timer.
     */
    void calibrateTimer();

    /**
     * @brief Retorna o custo medido (em microssegundos) de uma leitura de temporizador.
     */
    double getTimerOverheadUs() const;

    /**
     * @brief Reinicia a placa com mensagem de erro crítico.
     */
    static void fatalRestart(const char *why);

    // Métodos internos de acesso para o kernel FreeRTOS
    TaskContext* getTaskContext(int rank);
    TaskContext* getCurrentTaskContext();
    const HiveConfig& getConfig() const { return _config; }

private:
    HiveConfig            _config;
    bool                  _initialized;
    volatile bool         _kernelReady;
    HiveAppTaskFunction   _appTaskFunc;
    double                _timerOvhUs;

    TaskContext          *_taskContexts;
    int32_t              *_barrierArrivedGen;
    volatile int32_t     *_barrierWaitGen;
    int32_t               _coordGeneration;

    IPAddress            *_nodeIP;
    uint8_t             (*_nodeMAC)[6];
    volatile bool        *_nodeDiscovered;
    int64_t              *_lastDeliveredSeq;
    LinkStats            *_linkStats;
    PeerSnapshot         *_peerSnap;
    PeerSnapshot         *_peerRun;
    volatile int32_t     *_peerRunGen;

    QueueHandle_t         _senderQueue;
    SemaphoreHandle_t     _dataTxMutex;
    SemaphoreHandle_t     _controlTxMutex;
    SemaphoreHandle_t     _discoveryTxMutex;
    SemaphoreHandle_t     _barrierMutex;

    SemaphoreHandle_t     _dataAckSemaphore;
    SemaphoreHandle_t     _createAckSemaphore;
    SemaphoreHandle_t     _startAckSemaphore;

    volatile uint32_t     _nextSequence;
    portMUX_TYPE          _sequenceMux;
    volatile uint32_t     _lastAckSeq;
    volatile int          _lastCreateAckRank;
    volatile int          _lastCreateAckStatus;
    volatile int          _lastStartAckStatus;

    volatile uint32_t     _espnowRxDrop;
    volatile uint32_t     _macTxOk;
    volatile uint32_t     _macTxFail;

    uint32_t              _lastLoopStats;
    uint32_t              _lastLoopStackCheck;

    bool initTransport();
    void learnNode(int nodeId, const PeerAddr &addr);
    bool sendControlMsg(int node, const ControlMessage &msg);
    bool sendDataPkt(int node, const Message &msg);
    void sendDiscoveryMsg();
    bool transportRecv(int channel, void *buf, size_t size, PeerAddr &from, TickType_t wait);

    bool createLocalRankTask(int rank);
    void startLocalTasks();
    void printLocalRanks();

    void barrierReleaseLocal(int32_t generation);
    void barrierHandleArrival(int rank, int32_t generation);
    void sendBarrierRelease(int node, int32_t generation);

    bool sendCreateTaskCmd(int rank);
    bool sendStartCmd(int destinationNode);

    // FreeRTOS Task Entries
    static void kernelSenderTask(void *param);
    static void kernelReceiverTask(void *param);
    static void kernelDiscoveryTask(void *param);
    static void kernelControlTask(void *param);
    static void kernelMasterTask(void *param);
    static void rankTaskEntry(void *param);

    // Amigos e auxiliares
    friend int    MPI_Comm_rank(void);
    friend int    MPI_Comm_size(void);
    friend double MPI_Wtime(void);
    friend double MPI_Wtime_micro(void);
    friend bool   MPI_Send(const void *buffer, int count, int destinationRank);
    friend int    MPI_Recv(void *buffer, int maxCount, int sourceRank);
    friend void   MPI_Barrier(void);
    friend bool   MPI_Bcast(void *buffer, int count, int rootRank);
    friend bool   MPI_Finalize(void);
    friend bool   MPI_Finalized(void);

    friend void EspNowHandleRxStatic(const uint8_t *mac, const uint8_t *data, int len);
    friend void EspNowSendHandleStatic(const uint8_t *destMac, uint8_t status);
};

// Instância singleton global padrão
extern ESPHiveClass Hive;
extern ESPHiveClass ESPHive;

#endif // ESP_HIVE_H
