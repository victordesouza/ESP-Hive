#ifndef ESP_HIVE_CONFIG_H
#define ESP_HIVE_CONFIG_H

#include <Arduino.h>

// ============================================================
// MODOS DE COMUNICAÇÃO
// ============================================================
#define HIVE_COMM_WIFI              0
#define HIVE_COMM_ESPNOW            1

// Retrocompatibilidade
#ifndef COMM_WIFI
#define COMM_WIFI                   HIVE_COMM_WIFI
#endif
#ifndef COMM_ESPNOW
#define COMM_ESPNOW                 HIVE_COMM_ESPNOW
#endif

// ============================================================
// PORTAS E CANAIS DE REDE
// ============================================================
#ifndef HIVE_DATA_PORT
#define HIVE_DATA_PORT              5000
#endif

#ifndef HIVE_DISCOVERY_PORT
#define HIVE_DISCOVERY_PORT         5001
#endif

#ifndef HIVE_CONTROL_PORT
#define HIVE_CONTROL_PORT           5002
#endif

#ifndef HIVE_ESPNOW_CHANNEL
#define HIVE_ESPNOW_CHANNEL         2
#endif

#ifndef HIVE_ESPNOW_RX_QUEUE_LENGTH
#define HIVE_ESPNOW_RX_QUEUE_LENGTH 16
#endif

// ============================================================
// CARGA ÚTIL E FILAS
// ============================================================
#ifndef HIVE_MESSAGE_PAYLOAD
#define HIVE_MESSAGE_PAYLOAD        128     // Bytes de dados por mensagem MPI
#endif

#ifndef HIVE_QUEUE_LENGTH
#define HIVE_QUEUE_LENGTH           8       // Tamanho padrão da fila de recepção do rank
#endif

#ifndef HIVE_SENDER_QUEUE_LENGTH
#define HIVE_SENDER_QUEUE_LENGTH    32      // Fila global do transmissor do nó
#endif

#ifndef HIVE_PENDING_MAX
#define HIVE_PENDING_MAX            8       // Buffer para mensagens fora de ordem no rank
#endif

// ============================================================
// FREERTOS: PILHAS E PRIORIDADES
// ============================================================
#ifndef HIVE_APP_STACK_SIZE
#define HIVE_APP_STACK_SIZE         8192    // Stack para rank 0 e ranks de coordenação
#endif

#ifndef HIVE_APP_STACK_SIZE_SMALL
#define HIVE_APP_STACK_SIZE_SMALL   4096    // Stack para ranks secundários
#endif

#ifndef HIVE_KERNEL_STACK_SIZE
#define HIVE_KERNEL_STACK_SIZE      6144    // Stack para tarefas internas do kernel
#endif

#ifndef HIVE_TASK_PRIORITY
#define HIVE_TASK_PRIORITY          1       // Prioridade das tarefas de aplicação
#endif

#ifndef HIVE_KERNEL_TASK_PRIORITY
#define HIVE_KERNEL_TASK_PRIORITY   3       // Prioridade do Sender/Receiver/Control
#endif

#ifndef HIVE_MASTER_TASK_PRIORITY
#define HIVE_MASTER_TASK_PRIORITY   3       // Prioridade do Master (apenas no Node 0)
#endif

// ============================================================
// TIMEOUTS E CONFIABILIDADE
// ============================================================
#ifndef HIVE_DATA_ACK_TIMEOUT_MS
#define HIVE_DATA_ACK_TIMEOUT_MS    100     // Timeout para esperar ACK de dados
#endif

#ifndef HIVE_DATA_MAX_RETRIES
#define HIVE_DATA_MAX_RETRIES       10      // Tentativas máximas antes de considerar perdida
#endif

#ifndef HIVE_BARRIER_RESEND_MS
#define HIVE_BARRIER_RESEND_MS      300     // Reenvio de aviso de barreira ao Node 0
#endif

#ifndef HIVE_CONTROL_ACK_TIMEOUT_MS
#define HIVE_CONTROL_ACK_TIMEOUT_MS 2000    // Timeout para comandos do Master
#endif

#ifndef HIVE_CONTROL_MAX_RETRIES
#define HIVE_CONTROL_MAX_RETRIES    5       // Tentativas máximas de controle
#endif

// ============================================================
// PROTOCOLO INTERNO (TAGS E MAGIC BYTES)
// ============================================================
#define HIVE_DISCOVERY_MAGIC        0xB2
#define HIVE_ESPNOW_TAG_DATA        0xD1
#define HIVE_ESPNOW_TAG_CONTROL     0xC2
#define HIVE_ESPNOW_TAG_DISCOVERY   0xE3

#endif // ESP_HIVE_CONFIG_H
