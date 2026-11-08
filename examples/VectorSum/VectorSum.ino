/*
 * ============================================================
 * ESP-Hive: Exemplo de Soma de Vetor Distribuída (Map-Reduce)
 * ============================================================
 *
 * Demonstra a técnica clássica de dispersão (Scatter) e redução (Reduce):
 * 1. O Rank 0 define a quantidade total de elementos a somar.
 * 2. Cada Rank calcula a sua faixa de índices [start, start + count).
 * 3. Cada Rank gera/soma os elementos de sua fatia localmente.
 * 4. Os Workers enviam suas somas parciais para o Rank 0 via MPI_Send.
 * 5. O Rank 0 acumula as somas parciais e confere com a soma teórica:
 *       Soma(1..N) = N * (N + 1) / 2
 *
 * ============================================================
 */

#include <Arduino.h>
#include <ESPHive.h>

#define THIS_NODE_ID     0       // 0 para Master, 1..N-1 para Workers
#define NUM_NODES        2       // Quantidade de placas físicas
#define TOTAL_TASKS      4       // Total de ranks distribuídos

// Número total de elementos inteiros na soma
const uint64_t TOTAL_ELEMENTS = 1000000ULL; // 1 milhão de elementos

typedef struct __attribute__((packed))
{
    int32_t  srcRank;
    uint64_t partialSum;
    double   calcTimeMs;
} PartialResult;

void ApplicationTask(void *parameter)
{
    int rank = MPI_Comm_rank();
    int size = MPI_Comm_size();

    // Sincroniza todos os nós
    MPI_Barrier();

    if (rank == 0)
    {
        Serial.println();
        Serial.println("==================================================");
        Serial.println(" ESP-HIVE: SOMA DISTRIBUÍDA DE VETOR (MAP-REDUCE)");
        Serial.printf(" Total de elementos: %llu | Ranks: %d\n", TOTAL_ELEMENTS, size);
        Serial.println("==================================================");
    }

    // Divisão de trabalho uniforme entre os ranks
    uint64_t baseChunk = TOTAL_ELEMENTS / size;
    uint64_t remainder = TOTAL_ELEMENTS % size;

    uint64_t myCount = baseChunk + ((uint64_t)rank < remainder ? 1 : 0);
    uint64_t myStart = (uint64_t)rank * baseChunk + ((uint64_t)rank < remainder ? (uint64_t)rank : remainder) + 1;
    uint64_t myEnd   = myStart + myCount;

    double t0 = MPI_Wtime_micro();

    // Fase MAP: cálculo da soma parcial local
    uint64_t mySum = 0;
    for (uint64_t i = myStart; i < myEnd; i++)
    {
        mySum += i;

        // Cede a CPU a cada 100.000 iterações para alimentar o Watchdog do ESP32
        if ((i & 0x1FFFF) == 0)
        {
            vTaskDelay(1);
        }
    }

    double calcTimeMs = (MPI_Wtime_micro() - t0) / 1000.0;

    Serial.printf("[MAP] Rank %d (Node %d): faixa [%llu .. %llu] -> Soma Parcial = %llu (em %.2f ms)\n",
                  rank, Hive.getNodeId(), myStart, myEnd - 1, mySum, calcTimeMs);

    // Fase REDUCE: agregação no Rank 0
    if (rank == 0)
    {
        uint64_t totalSum = mySum;
        double redStart = MPI_Wtime_micro();

        for (int r = 1; r < size; r++)
        {
            PartialResult res;
            int n = MPI_Recv(&res, sizeof(res), MPI_ANY_SOURCE);

            if (n == sizeof(res))
            {
                totalSum += res.partialSum;
                Serial.printf("[REDUCE] Rank 0 recebeu do Rank %d: %llu (tempo: %.2f ms)\n",
                              res.srcRank, res.partialSum, res.calcTimeMs);
            }
            else
            {
                Serial.printf("[REDUCE] ERRO recebendo resultado parcial do Rank %d!\n", r);
            }
        }

        double totalTimeMs = (MPI_Wtime_micro() - t0) / 1000.0;

        // Conferência com a fórmula analítica de Gauss: S = N * (N + 1) / 2
        uint64_t expectedSum = (TOTAL_ELEMENTS * (TOTAL_ELEMENTS + 1ULL)) / 2ULL;

        Serial.println("--------------------------------------------------");
        Serial.printf("[RESULTADO] Soma Calculada pelo Cluster = %llu\n", totalSum);
        Serial.printf("[RESULTADO] Soma Analítica Esperada      = %llu\n", expectedSum);
        Serial.printf("[RESULTADO] Status: %s\n", (totalSum == expectedSum) ? "SUCESSO (100% CORRETO)" : "FALHA (DIVERGÊNCIA)");
        Serial.printf("[RESULTADO] Tempo Total de Execução: %.2f ms\n", totalTimeMs);
        Serial.println("--------------------------------------------------");
    }
    else
    {
        // Envia a soma parcial para o Rank 0
        PartialResult res;
        res.srcRank    = rank;
        res.partialSum = mySum;
        res.calcTimeMs = calcTimeMs;

        MPI_Send(&res, sizeof(res), 0);
    }

    MPI_Barrier();
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
    cfg.commMode   = HIVE_MODE_ESPNOW;

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
