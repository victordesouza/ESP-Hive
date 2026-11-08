/*
 * ============================================================
 * ESP-Hive: Benchmark de Monte Carlo Distribuído (Cálculo de Pi)
 * ============================================================
 *
 * Este exemplo demonstra a execução de uma aplicação paralela CPU-bound
 * no cluster ESP32 utilizando a biblioteca ESP-Hive.
 *
 * FUNCIONAMENTO:
 * 1. O Rank 0 atua como coordenador e define a divisão de trabalho.
 * 2. Amostras pseudoaleatórias do círculo unitário são calculadas
 *    paralelamente entre os nós e ranks ativos.
 * 3. Cada worker acumula seus acertos e envia o resultado parcial ao Rank 0.
 * 4. O Rank 0 agrega os resultados e calcula a estimativa de Pi:
 *       Pi ≈ 4 * (acertos / N)
 * 5. O benchmark varia o número de ranks R em {1, 2, 4, 8, 16, 32}
 *    e computa métricas de Speedup e Eficiência paralela, além de
 *    monitorar perda de pacotes e retransmissões.
 *
 * CONFIGURAÇÃO:
 * - Ajuste THIS_NODE_ID para cada placa (0 para Master, 1..3 para Workers).
 * - Mantenha NUM_NODES e TOTAL_APPLICATION_TASKS idênticos em todas as placas.
 * - Modo de comunicação: HIVE_COMM_ESPNOW (recomendado) ou HIVE_COMM_WIFI.
 * ============================================================
 */

#include <Arduino.h>
#include <ESPHive.h>

// ============================================================
// CONFIGURAÇÃO DO CLUSTER
// ============================================================
#define THIS_NODE_ID                0       // <<< 0 no Master; 1, 2, 3... nas outras placas
#define NUM_NODES                   4       // Número de placas físicas (1 = standalone/local)
#define TOTAL_APPLICATION_TASKS     16      // Total de ranks distribuídos
#define COMM_MODE                   HIVE_COMM_ESPNOW // HIVE_COMM_ESPNOW ou HIVE_COMM_WIFI

// Configurações Wi-Fi (usadas somente se COMM_MODE == HIVE_COMM_WIFI)
const char *WIFI_SSID     = "SuaRedeWiFi";
const char *WIFI_PASSWORD = "SuaSenhaWiFi";

// ============================================================
// PARÂMETROS DO BENCHMARK
// ============================================================
#define BENCH_TOTAL_SAMPLES         40000000UL   // Amostras totais (ajuste para T1 ≈ 5..20 s)
#define BENCH_REPS                  1            // Repetições por configuração de ranks
#define BENCH_PRINT_RANKS           1            // 1 = imprime tempos individuais de cada rank
#define BENCH_PRINT_RETX_PER_RUN    1            // 1 = imprime retransmissões por execução
#define BENCH_SETTLE_MS             300          // Estabilização entre repetições
#define BENCH_SEED                  0x1234ABCDu
#define BENCH_SLICE_SAMPLES         262144u      // Fatia de yield para alimentar o Watchdog do ESP32

static const int kRanks[] = {1, 2, 4, 8, 16, 32};
#define BENCH_N_RANKS   ((int)(sizeof(kRanks) / sizeof(kRanks[0])))

#if COMM_MODE == HIVE_COMM_ESPNOW
#define COMM_NAME "ESPNOW"
#else
#define COMM_NAME "UDP"
#endif

// ============================================================
// ESTRUTURAS DO BENCHMARK
// ============================================================
typedef struct __attribute__((packed))
{
    uint32_t start;
    uint32_t count;
    uint32_t seed;
} BenchTask;

typedef struct
{
    int64_t  sum;
    uint32_t commUs;
    uint32_t calcUs;
    int32_t  rank;
} BenchPartial;

typedef struct
{
    double   totalUs, genUs, commUs, calcUs, redUs;
    int64_t  global, ref;
    int      sendFail;
    bool     ok;
    uint32_t rankCalcUs[TOTAL_APPLICATION_TASKS];
    uint32_t rankCommUs[TOTAL_APPLICATION_TASKS];
} BenchRaw;

typedef struct
{
    uint64_t msgs;
    uint64_t tx;
    uint64_t retx;
    uint64_t failed;
} RetxAccum;

typedef struct
{
    int      R, reps;
    double   total, gen, comm, calc0, calcMax, commMax, red;
    double   speedup, eff, speedupCalc;
    uint64_t msgs, tx, retx, failed;
    bool     ok;
} ResultRow;

static ResultRow gRows[6];
static int       gRowCount = 0;
static double    gT1Total = 0;
static double    gT1Calc  = 0;
static int64_t   gRefHits = -1;

static inline int64_t NowUs() { return esp_timer_get_time(); }
static inline double  Pos(double v) { return v > 0.0 ? v : 0.0; }

// ============================================================
// NÚCLEO DE CÁLCULO PSEUDOALEATÓRIO (LOWBIAS32)
// ============================================================
static inline uint32_t Bench_Hash(uint32_t x)
{
    x ^= x >> 16;  x *= 0x7feb352dU;
    x ^= x >> 15;  x *= 0x846ca68bU;
    x ^= x >> 16;
    return x;
}

static uint32_t Bench_CountHits(uint32_t start, uint32_t count, uint32_t seed, double &calcUs)
{
    uint32_t hits = 0;
    uint32_t done = 0;

    while (done < count)
    {
        uint32_t n = count - done;
        if (n > BENCH_SLICE_SAMPLES) n = BENCH_SLICE_SAMPLES;

        int64_t t0 = NowUs();

        uint32_t h = 0;
        uint32_t i = start + done;
        const uint32_t e = i + n;

        for (; i < e; i++)
        {
            uint32_t a = Bench_Hash(2u * i + seed);
            uint32_t b = Bench_Hash(2u * i + 1u + seed);
            uint32_t x = a >> 17;                    // Faixa 0..32767
            uint32_t y = b >> 17;
            h += (x * x + y * y) < (1u << 30);       // Raio ao quadrado (32768²)
        }

        hits   += h;
        calcUs += (double)(NowUs() - t0);
        done   += n;

        vTaskDelay(1); // Cede a CPU brevemente para alimentar o Task Watchdog
    }

    return hits;
}

static void Bench_Range(int rank, int R, uint32_t N, uint32_t *start, uint32_t *count)
{
    uint32_t base = N / R;
    uint32_t rem  = N % R;
    *count = base + ((uint32_t)rank < rem ? 1 : 0);
    *start = (uint32_t)rank * base + ((uint32_t)rank < rem ? (uint32_t)rank : rem);
}

// ============================================================
// LÓGICA DO MASTER (RANK 0) E WORKERS
// ============================================================
static void Bench_Rank0(uint32_t N, int R, BenchRaw &o)
{
    const double ovh = Hive.getTimerOverheadUs();
    double commUs = 0, calcUs = 0, redUs = 0;
    int sendFail = 0;
    bool ok = true;

    int64_t tStart = NowUs();

    // 1. Envia os descritores de tarefa para os workers
    int64_t c0 = NowUs();
    for (int dst = 1; dst < R; dst++)
    {
        BenchTask t;
        Bench_Range(dst, R, N, &t.start, &t.count);
        t.seed = BENCH_SEED;
        if (!MPI_Send(&t, sizeof(t), dst)) sendFail++;
    }
    commUs += (double)(NowUs() - c0) - ovh;

    // 2. Calcula a fatia correspondente ao Rank 0 (em paralelo com os workers)
    uint32_t s0, c;
    Bench_Range(0, R, N, &s0, &c);
    int64_t global = Bench_CountHits(s0, c, BENCH_SEED, calcUs);

    memset(o.rankCalcUs, 0, sizeof(o.rankCalcUs));
    memset(o.rankCommUs, 0, sizeof(o.rankCommUs));
    o.rankCalcUs[0] = (uint32_t)Pos(calcUs);
    o.rankCommUs[0] = (uint32_t)Pos(commUs);

    // 3. Redução: recebe as somas parciais de todos os workers
    int64_t r0 = NowUs();
    for (int k = 1; k < R; k++)
    {
        BenchPartial pm;
        int n = MPI_Recv(&pm, sizeof(pm), MPI_ANY_SOURCE);

        if (n != (int)sizeof(pm) || pm.rank < 1 || pm.rank >= R)
        {
            ok = false;
            continue;
        }

        global += pm.sum;
        o.rankCalcUs[pm.rank] = pm.calcUs;
        o.rankCommUs[pm.rank] = pm.commUs;
    }
    redUs = (R > 1) ? ((double)(NowUs() - r0) - ovh) : 0.0;

    o.totalUs = (double)(NowUs() - tStart);
    o.genUs   = 0.0;
    o.commUs  = Pos(commUs);
    o.calcUs  = Pos(calcUs);
    o.redUs   = Pos(redUs);
    o.global  = global;

    if (R == 1) gRefHits = global; // R=1 define o gabarito de referência
    o.ref      = gRefHits;
    o.sendFail = sendFail;
    o.ok       = ok && (sendFail == 0) && (global == gRefHits);
}

static void Bench_Worker(int rank)
{
    const double ovh = Hive.getTimerOverheadUs();

    BenchTask t;
    int64_t c0 = NowUs();
    int n = MPI_Recv(&t, sizeof(t), 0);
    double commUs = (double)(NowUs() - c0) - ovh;

    BenchPartial pm;
    memset(&pm, 0, sizeof(pm));

    double calcUs = 0;
    if (n == (int)sizeof(t))
        pm.sum = Bench_CountHits(t.start, t.count, t.seed, calcUs);
    else
        pm.sum = -1;

    pm.commUs = (uint32_t)Pos(commUs);
    pm.calcUs = (uint32_t)Pos(calcUs);
    pm.rank   = rank;

    MPI_Send(&pm, sizeof(pm), 0);
}

static void Bench_Run(int rank, uint32_t N, int R, BenchRaw &raw)
{
    int64_t result = 0;

    if (rank >= R)
    {
        // Ranks inativos nesta rodada: aguardam no Bcast
    }
    else if (rank == 0)
    {
        Bench_Rank0(N, R, raw);
        result = raw.global;
    }
    else
    {
        Bench_Worker(rank);
    }

    MPI_Bcast(&result, sizeof(result), 0);
}

// ============================================================
// RELATÓRIOS E FORMATAÇÃO CSV
// ============================================================
static void Bench_TakeBase(LinkStats *base)
{
    for (int n = 0; n < NUM_NODES; n++)
    {
        LinkStats *ls = Hive.getLinkStats(n);
        if (ls) base[n] = *ls;
    }
}

static void Bench_RetxReport(uint32_t N, int R, int rep, const LinkStats *base, RetxAccum &acc)
{
    if (NUM_NODES == 1) return;

    for (int n = 0; n < NUM_NODES; n++)
    {
        if (n == THIS_NODE_ID) continue;

        LinkStats *ls = Hive.getLinkStats(n);
        if (!ls) continue;

        uint32_t msgs   = ls->msgs       - base[n].msgs;
        uint32_t tx     = ls->txAttempts - base[n].txAttempts;
        uint32_t failed = ls->msgFailed  - base[n].msgFailed;
        uint32_t retx   = (tx > msgs) ? (tx - msgs) : 0;

        acc.msgs += msgs;  acc.tx += tx;  acc.retx += retx;  acc.failed += failed;

#if BENCH_PRINT_RETX_PER_RUN
        if (msgs || tx)
        {
            Serial.printf("[RETX] node%d->node%d N=%lu R=%d rep=%d | mensagens=%lu tentativas=%lu retransmissoes=%lu perdidas=%lu\n",
                          THIS_NODE_ID, n, (unsigned long)N, R, rep + 1,
                          (unsigned long)msgs, (unsigned long)tx,
                          (unsigned long)retx, (unsigned long)failed);
        }
#endif
    }
}

#define BENCH_CSV_RES_HEADER \
    "[CSV_RES_HEADER] modo,nodes,N,ranks,reps,Ttotal_ms,Tgen_ms,Tcomm_ms,Tcalc0_ms," \
    "TcalcMax_ms,TcommMax_ms,Tred_ms,Speedup,Eficiencia,SpeedupCalc," \
    "mensagens,tentativas,retransmissoes,msgPerdidaDeVez,ok"

static void Bench_PrintHeader()
{
    Serial.println();
    Serial.println("==========================================================");
    Serial.println(" ESP-HIVE BENCHMARK: MONTE CARLO PI (Cluster ESP32 MPI)");
    Serial.println("==========================================================");
    Serial.printf("[BENCH] Modo=%s  nodes=%d  ranks_totais=%d\n", COMM_NAME, NUM_NODES, TOTAL_APPLICATION_TASKS);
    Serial.printf("[BENCH] Amostras: N=%lu | yield=%u amostras\n",
                  (unsigned long)BENCH_TOTAL_SAMPLES, (unsigned)BENCH_SLICE_SAMPLES);

    Serial.print("[BENCH] Ranks avaliados R:");
    for (int i = 0; i < BENCH_N_RANKS; i++)
        if (kRanks[i] <= TOTAL_APPLICATION_TASKS) Serial.printf(" %d", kRanks[i]);
    Serial.println();

    Serial.printf("[BENCH] Repetições por R: %d\n", BENCH_REPS);
    Serial.println("[BENCH] Tempos em ms. Tgen=0. Speedup = T1/TR.");
    Serial.println("[CSV_RUN_HEADER] modo,nodes,N,ranks,rep,Ttotal_ms,Tgen_ms,Tcomm_ms,Tcalc0_ms,TcalcMax_ms,TcommMax_ms,Tred_ms,acertos,ok");
    Serial.println(BENCH_CSV_RES_HEADER);
    Serial.println();
}

static void Bench_PrintRowCsv(const ResultRow &r)
{
    Serial.printf("[CSV_RES],%s,%d,%lu,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.4f,%.4f,%.4f,%lu,%lu,%lu,%lu,%d\n",
                  COMM_NAME, NUM_NODES, (unsigned long)BENCH_TOTAL_SAMPLES, r.R, r.reps,
                  r.total, r.gen, r.comm, r.calc0, r.calcMax, r.commMax, r.red,
                  r.speedup, r.eff, r.speedupCalc,
                  (unsigned long)r.msgs, (unsigned long)r.tx,
                  (unsigned long)r.retx, (unsigned long)r.failed, r.ok ? 1 : 0);
}

static void Bench_PrintRowPretty(const ResultRow &r)
{
    Serial.println("----------------------------------------------------------");
    Serial.printf("[RESULT] N=%lu ranks=%d nodes=%d %s (media de %d exec) %s\n",
                  (unsigned long)BENCH_TOTAL_SAMPLES, r.R, NUM_NODES, COMM_NAME, r.reps,
                  r.ok ? "OK" : "*** INCORRETO ***");
    Serial.printf("[RESULT]   Ttotal=%.3f ms | Tcomm=%.3f | Tcalc0=%.3f (max %.3f) | Tred=%.3f\n",
                  r.total, r.comm, r.calc0, r.calcMax, r.red);
    Serial.printf("[RESULT]   Speedup=%.3f | Eficiencia=%.3f | SpeedupCalc=%.3f\n",
                  r.speedup, r.eff, r.speedupCalc);
    Serial.printf("[RESULT]   (node 0) msgs=%lu tentativas=%lu retx=%lu perdas=%lu\n",
                  (unsigned long)r.msgs, (unsigned long)r.tx, (unsigned long)r.retx, (unsigned long)r.failed);
    Bench_PrintRowCsv(r);
}

// ============================================================
// APPLICATION TASK (ENTRYPOINT DOS RANKS NO ESP-HIVE)
// ============================================================
void ApplicationTask(void *parameter)
{
    const int      rank      = MPI_Comm_rank();
    const uint32_t N         = BENCH_TOTAL_SAMPLES;
    const bool     master    = (rank == 0);
    const bool     statsRank = (rank == THIS_NODE_ID);

    if (master) Bench_PrintHeader();
    if (statsRank) Hive.resetStats();

    RetxAccum acc;
    LinkStats base[NUM_NODES];

    MPI_Barrier();

    for (int ri = 0; ri < BENCH_N_RANKS; ri++)
    {
        const int R = kRanks[ri];
        if (R > TOTAL_APPLICATION_TASKS) continue;

        memset(&acc, 0, sizeof(acc));

        double sTotal = 0, sGen = 0, sComm = 0, sCalc0 = 0,
               sCalcMax = 0, sCommMax = 0, sRed = 0;
        bool   allOk = true;

        if (master)
            Serial.printf("\n[BENCH] >>> N=%lu  ranks=%d  (%d execuções)\n",
                          (unsigned long)N, R, BENCH_REPS);

        for (int rep = 0; rep < BENCH_REPS; rep++)
        {
            if (statsRank) Bench_TakeBase(base);

            MPI_Barrier();

            BenchRaw raw;
            memset(&raw, 0, sizeof(raw));

            Bench_Run(rank, N, R, raw);

            MPI_Barrier();

            if (master)
            {
                double calcMax = raw.rankCalcUs[0];
                double commMax = raw.rankCommUs[0];

                for (int r = 1; r < R; r++)
                {
                    if (raw.rankCalcUs[r] > calcMax) calcMax = raw.rankCalcUs[r];
                    if (raw.rankCommUs[r] > commMax) commMax = raw.rankCommUs[r];
                }

                double total = raw.totalUs / 1000.0;
                double gen   = raw.genUs   / 1000.0;
                double comm  = raw.commUs  / 1000.0;
                double calc0 = raw.calcUs  / 1000.0;
                double red   = raw.redUs   / 1000.0;
                calcMax /= 1000.0;
                commMax /= 1000.0;

                sTotal += total;   sGen += gen;         sComm += comm;
                sCalc0 += calc0;   sCalcMax += calcMax; sCommMax += commMax;
                sRed += red;
                allOk = allOk && raw.ok;

                double piEst = 4.0 * (double)raw.global / (double)N;

                Serial.printf("[RUN] N=%lu ranks=%d nodes=%d %s rep=%d/%d | Ttotal=%.3f Tcomm=%.3f Tcalc0=%.3f TcalcMax=%.3f Tred=%.3f ms | pi≈%.6f %s\n",
                              (unsigned long)N, R, NUM_NODES, COMM_NAME, rep + 1, BENCH_REPS,
                              total, comm, calc0, calcMax, red, piEst,
                              raw.ok ? "OK" : "*** ERRO ***");

                Serial.printf("[CSV_RUN],%s,%d,%lu,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%lld,%d\n",
                              COMM_NAME, NUM_NODES, (unsigned long)N, R, rep + 1,
                              total, gen, comm, calc0, calcMax, commMax, red,
                              (long long)raw.global, raw.ok ? 1 : 0);

#if BENCH_PRINT_RANKS
                for (int r = 0; r < R; r++)
                    Serial.printf("[RANK] rank %2d (node %d): Tcalc=%.3f ms  Tcomm=%.3f ms\n",
                                  r, Hive.rankToNode(r),
                                  raw.rankCalcUs[r] / 1000.0, raw.rankCommUs[r] / 1000.0);
#endif
            }

            if (statsRank)
            {
                vTaskDelay(pdMS_TO_TICKS(BENCH_SETTLE_MS));
                Bench_RetxReport(N, R, rep, base, acc);
            }
        }

        if (master)
        {
            ResultRow row;
            memset(&row, 0, sizeof(row));

            row.R       = R;
            row.reps    = BENCH_REPS;
            row.total   = sTotal   / BENCH_REPS;
            row.gen     = sGen     / BENCH_REPS;
            row.comm    = sComm    / BENCH_REPS;
            row.calc0   = sCalc0   / BENCH_REPS;
            row.calcMax = sCalcMax / BENCH_REPS;
            row.commMax = sCommMax / BENCH_REPS;
            row.red     = sRed     / BENCH_REPS;
            row.ok      = allOk;

            if (R == 1)
            {
                gT1Total = row.total;
                gT1Calc  = row.calc0;
            }

            row.speedup     = (gT1Total > 0 && row.total > 0)  ? gT1Total / row.total   : -1.0;
            row.eff         = (row.speedup >= 0) ? row.speedup / R : -1.0;
            row.speedupCalc = (gT1Calc > 0 && row.calcMax > 0) ? gT1Calc / row.calcMax : -1.0;

            row.msgs   = acc.msgs;
            row.tx     = acc.tx;
            row.retx   = acc.retx;
            row.failed = acc.failed;

            if (gRowCount < (int)(sizeof(gRows) / sizeof(gRows[0])))
                gRows[gRowCount++] = row;

            Bench_PrintRowPretty(row);
        }
    }

    MPI_Barrier();
    vTaskDelay(pdMS_TO_TICKS(1000));

    if (master)
    {
        Serial.println();
        Serial.println("==================== RESUMO FINAL (CSV) ====================");
        Serial.println(BENCH_CSV_RES_HEADER);
        for (int i = 0; i < gRowCount; i++) Bench_PrintRowCsv(gRows[i]);
        Serial.println("============================================================");
    }

    Serial.printf("[BENCH] Rank %d: fim do benchmark\n", rank);
    MPI_Finalize();
}

// ============================================================
// SETUP & LOOP (ARDUINO STANDARD)
// ============================================================
void setup()
{
    Serial.begin(115200);
    delay(1000);

    HiveConfig cfg;
    cfg.nodeId       = THIS_NODE_ID;
    cfg.numNodes     = NUM_NODES;
    cfg.totalTasks   = TOTAL_APPLICATION_TASKS;
    cfg.commMode     = (COMM_MODE == HIVE_COMM_ESPNOW) ? HIVE_MODE_ESPNOW : HIVE_MODE_WIFI;
    cfg.wifiSSID     = WIFI_SSID;
    cfg.wifiPassword = WIFI_PASSWORD;

    if (!Hive.begin(cfg))
    {
        Serial.println("Falha ao inicializar cluster ESP-Hive!");
        while (true) delay(1000);
    }
}

void loop()
{
    Hive.update();
}
