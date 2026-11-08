#ifndef ESP_HIVE_TYPES_H
#define ESP_HIVE_TYPES_H

#include <Arduino.h>
#include <WiFi.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>
#include "ESPHive_Config.h"

// ============================================================
// MODOS DE OPERAÇÃO
// ============================================================
enum HiveCommMode
{
    HIVE_MODE_WIFI   = HIVE_COMM_WIFI,
    HIVE_MODE_ESPNOW = HIVE_COMM_ESPNOW
};

// ============================================================
// TIPOS DE MENSAGENS DE DADOS
// ============================================================
enum MessageType
{
    MSG_APPLICATION = 1,
    MSG_DATA_ACK    = 2
};

typedef struct __attribute__((packed))
{
    uint8_t  type;
    int32_t  srcRank;
    int32_t  dstRank;
    uint32_t sequence;
    uint16_t length;
    char     payload[HIVE_MESSAGE_PAYLOAD];
} Message;

// ============================================================
// MENSAGENS DE CONTROLE DO KERNEL
// ============================================================
enum ControlType
{
    CONTROL_CREATE_TASK     = 1,
    CONTROL_CREATE_ACK      = 2,
    CONTROL_START           = 3,
    CONTROL_START_ACK       = 4,
    CONTROL_BARRIER         = 5,
    CONTROL_BARRIER_RELEASE = 6,
    CONTROL_STATS           = 7     // Snapshot de estatísticas do link
};

typedef struct __attribute__((packed))
{
    uint8_t  type;
    uint8_t  srcNode;
    int32_t  rank;
    int32_t  generation;    // em CONTROL_STATS: 0 = acumulado, >0 = id da execução
    int32_t  status;
    uint32_t stat[4];       // só usado em CONTROL_STATS
} ControlMessage;

// ============================================================
// MENSAGEM DE DESCOBERTA (DISCOVERY)
// ============================================================
typedef struct __attribute__((packed))
{
    uint8_t magic;
    uint8_t nodeId;
    uint8_t ip[4];
} DiscoveryMessage;

// ============================================================
// CANAIS DE TRANSPORTE
// ============================================================
enum CommChannel
{
    CH_DATA      = 0,
    CH_CONTROL   = 1,
    CH_DISCOVERY = 2
};

// ============================================================
// ENDEREÇO DE NÓ PARCEIRO
// ============================================================
typedef struct
{
    IPAddress ip;
    uint8_t   mac[6];
} PeerAddr;

// ============================================================
// CONTEXTO DE CADA RANK (APPLICATION TASK)
// ============================================================
typedef struct
{
    int               rank;
    TaskHandle_t      taskHandle;
    QueueHandle_t     receiveQueue;

    SemaphoreHandle_t barrierSem;
    int32_t           barrierGen;

    Message          *pending;
    int               pendingCount;

    volatile bool     finalized;
} TaskContext;

// ============================================================
// ESTATÍSTICAS POR LINK DE COMUNICAÇÃO
// ============================================================
typedef struct
{
    // Escritos pelo TaskSender
    volatile uint32_t msgs;         // Mensagens remotas processadas
    volatile uint32_t txAttempts;   // Pacotes de dados que realmente saíram
    volatile uint32_t txSendFail;   // Falha local de TX (hardware/buffer)
    volatile uint32_t ackOk;        // Tentativas que receberam ACK com sucesso
    volatile uint32_t msgFailed;    // Mensagens perdidas definitivamente (sem ACK)

    // Escritos pelo TaskReceiver
    volatile uint32_t rxData;       // Pacotes de dados válidos recebidos
    volatile uint32_t rxUnique;     // Mensagens entregues pela primeira vez
    volatile uint32_t rxDup;        // Duplicatas recebidas (ACK anterior se perdeu)
    volatile uint32_t rxQueueFull;  // Descartados: fila do rank receptora cheia
    volatile uint32_t ackTx;        // ACKs enviados de volta
    volatile uint32_t ackRx;        // ACKs recebidos
} LinkStats;

// Snapshot enviado pelo vizinho (visão dele do link com este nó)
typedef struct
{
    uint32_t      txAttempts;       // Dados que ele enviou para este nó
    uint32_t      rxData;           // Dados que ele recebeu deste nó
    uint32_t      ackTx;            // ACKs que ele enviou para este nó
    uint32_t      ackRx;            // ACKs que ele recebeu deste nó
    volatile bool valid;
} PeerSnapshot;

// Pacote interno para fila do ESP-NOW
typedef struct
{
    uint8_t  mac[6];
    uint16_t len;
    uint8_t  data[sizeof(Message)];
} EspNowRxPacket;

// ============================================================
// CONFIGURAÇÃO DO CLUSTER ESP-HIVE
// ============================================================
struct HiveConfig
{
    int          nodeId;             // ID deste nó: 0 (Master), 1, 2, ...
    int          numNodes;           // Número total de placas no cluster
    int          totalTasks;         // Total de ranks MPI (distribuídos entre nós)
    HiveCommMode commMode;           // HIVE_MODE_ESPNOW ou HIVE_MODE_WIFI

    // Configurações Wi-Fi (usadas se commMode == HIVE_MODE_WIFI)
    const char  *wifiSSID;
    const char  *wifiPassword;

    // Configurações ESP-NOW (usadas se commMode == HIVE_MODE_ESPNOW)
    uint8_t      espnowChannel;

    // Configurações avançadas de FreeRTOS
    uint32_t     appStackSize;       // Stack para ranks prioritários (padrão: 8192)
    uint32_t     appStackSizeSmall;  // Stack para ranks menores (padrão: 4096)
    uint32_t     kernelStackSize;    // Stack para tarefas internas do kernel (padrão: 6144)
    uint32_t     statsPrintPeriodMs; // 0 = desativado, >0 = imprime stats periódicos no loop()
    bool         debugStack;         // Monitora o uso de stack em tempo de execução
    bool         verbose;            // Imprime logs detalhados de barreiras e mensagens

    HiveConfig() :
        nodeId(0),
        numNodes(1),
        totalTasks(1),
        commMode(HIVE_MODE_ESPNOW),
        wifiSSID(""),
        wifiPassword(""),
        espnowChannel(HIVE_ESPNOW_CHANNEL),
        appStackSize(HIVE_APP_STACK_SIZE),
        appStackSizeSmall(HIVE_APP_STACK_SIZE_SMALL),
        kernelStackSize(HIVE_KERNEL_STACK_SIZE),
        statsPrintPeriodMs(0),
        debugStack(false),
        verbose(false)
    {}
};

#endif // ESP_HIVE_TYPES_H
