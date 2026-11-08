/*
 * ============================================================
 * ESP-Hive: Exemplo Básico de Passagem de Token em Anel (Ring)
 * ============================================================
 *
 * Este exemplo implementa a clássica topologia em anel MPI:
 * - O Rank 0 inicializa um "token" (contador = 1).
 * - O Rank 0 envia o token para o Rank 1.
 * - Cada Rank (i) recebe o token do Rank (i-1), incrementa em 1,
 *   e repassa para o Rank (i+1).
 * - O último Rank (Size - 1) envia de volta para o Rank 0, completando uma volta.
 * - Isso demonstra MPI_Send, MPI_Recv e a comunicação entre nós diferentes!
 *
 * ============================================================
 */

#include <Arduino.h>
#include <ESPHive.h>

// Ajuste para cada nó físico: 0 no Master, 1..N-1 nos Workers
#define THIS_NODE_ID     0
#define NUM_NODES        2       // Total de placas ESP32 (teste com 2 placas ou 1 em standalone)
#define TOTAL_TASKS      4       // Total de ranks distribuídos

void ApplicationTask(void *parameter)
{
    int rank = MPI_Comm_rank();
    int size = MPI_Comm_size();

    int nextRank = (rank + 1) % size;
    int prevRank = (rank - 1 + size) % size;

    const int TOTAL_LAPS = 5;

    Serial.printf("[RING] Rank %d iniciado (Node %d). Vizinhos: Prev=%d, Next=%d\n",
                  rank, Hive.getNodeId(), prevRank, nextRank);

    // Sincroniza todos os nós antes de iniciar a passagem do bastão
    MPI_Barrier();

    for (int lap = 1; lap <= TOTAL_LAPS; lap++)
    {
        int token = 0;

        if (rank == 0)
        {
            token = lap * 100;
            double t0 = MPI_Wtime_micro();

            Serial.printf("\n[RING] --- Volta %d/%d iniciada pelo Rank 0 (Token inicial = %d) ---\n",
                          lap, TOTAL_LAPS, token);

            // Rank 0 envia para Rank 1
            MPI_Send(&token, sizeof(token), nextRank);

            // Rank 0 aguarda o token dar a volta completa em todos os nós
            MPI_Recv(&token, sizeof(token), prevRank);

            double elapsedMs = (MPI_Wtime_micro() - t0) / 1000.0;
            Serial.printf("[RING] Volta %d completada! Token final recebido = %d | Tempo: %.2f ms\n",
                          lap, token, elapsedMs);
        }
        else
        {
            // Workers recebem do rank anterior, incrementam e passam para o próximo
            MPI_Recv(&token, sizeof(token), prevRank);

            Serial.printf("[RING] Rank %d recebeu token = %d do Rank %d -> repassando para Rank %d\n",
                          rank, token, prevRank, nextRank);

            token++;

            MPI_Send(&token, sizeof(token), nextRank);
        }

        MPI_Barrier();
        vTaskDelay(pdMS_TO_TICKS(500));
    }

    if (rank == 0)
    {
        Serial.println("\n[RING] Teste do anel concluído com sucesso em todas as voltas!");
    }

    MPI_Finalize();
}

void setup()
{
    Serial.begin(115200);
    delay(1000);

    HiveConfig cfg;
    cfg.nodeId     = THIS_NODE_ID;
    cfg.numNodes   = NUM_NODES;
    cfg.totalTasks = TOTAL_TASKS;
    cfg.commMode   = HIVE_MODE_ESPNOW; // ESP-NOW sem roteador

    if (!Hive.begin(cfg))
    {
        Serial.println("Erro ao inicializar ESP-Hive!");
        while (true) delay(1000);
    }
}

void loop()
{
    Hive.update();
}
