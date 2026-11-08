# Guia de Programação do Cluster ESP-Hive

Bem-vindo ao **Guia Oficial de Programação do ESP-Hive**. Este manual foi elaborado para orientar desenvolvedores, pesquisadores e entusiastas na criação de aplicações distribuídas e paralelas de alto desempenho utilizando clusters de placas ESP32 no ambiente Arduino IDE.

---

## Sumário
1. [Introdução e Filosofia do ESP-Hive](#1-introdução-e-filosofia-do-esp-hive)
2. [Arquitetura de Hardware e Rede](#2-arquitetura-de-hardware-e-rede)
   - [Nós Físicos vs Ranks Virtuais](#nós-físicos-vs-ranks-virtuais)
   - [Modos de Comunicação: ESP-NOW vs Wi-Fi UDP](#modos-de-comunicação-esp-now-vs-wi-fi-udp)
   - [Modo Standalone (Desenvolvimento em 1 Placa)](#modo-standalone-desenvolvimento-em-1-placa)
3. [Instalação e Configuração no Arduino IDE](#3-instalação-e-configuração-no-arduino-ide)
4. [Estrutura Básica de um Programa ESP-Hive](#4-estrutura-básica-de-um-programa-esp-hive)
5. [Referência Completa da API MPI](#5-referência-completa-da-api-mpi)
   - [`MPI_Comm_rank()`](#mpi_comm_rank)
   - [`MPI_Comm_size()`](#mpi_comm_size)
   - [`MPI_Send()`](#mpi_send)
   - [`MPI_Recv()`](#mpi_recv)
   - [`MPI_Barrier()`](#mpi_barrier)
   - [`MPI_Bcast()`](#mpi_bcast)
   - [`MPI_Finalize()` e `MPI_Finalized()`](#mpi_finalize-e-mpi_finalized)
   - [`MPI_Wtime()` e `MPI_Wtime_micro()`](#mpi_wtime-e-mpi_wtime_micro)
6. [Padrões de Programação Distribuída](#6-padrões-de-programação-distribuída)
   - [Padrão 1: Master-Worker (Divisão de Fatias / Task Farming)](#padrão-1-master-worker-divisão-de-fatias--task-farming)
   - [Padrão 2: Map-Reduce (Redução de Dados)](#padrão-2-map-reduce-redução-de-dados)
   - [Padrão 3: Passagem de Bastão em Anel (Pipeline / Ring)](#padrão-3-passagem-de-bastão-em-anel-pipeline--ring)
7. [Boas Práticas de Sistemas Embarcados e FreeRTOS](#7-boas-práticas-de-sistemas-embarcados-e-freertos)
   - [O Task Watchdog do ESP32 e o Fatiamento de CPU](#o-task-watchdog-do-esp32-e-o-fatiamento-de-cpu)
   - [Dimensionamento de Pilha (Stack) e Limite de Memória RAM](#dimensionamento-de-pilha-stack-e-limite-de-memória-ram)
   - [Envio de Cargas Maiores que 128 Bytes (Streaming de Mensagens)](#envio-de-cargas-maiores-que-128-bytes-streaming-de-mensagens)
8. [Métricas de Desempenho e Diagnóstico de Rede](#8-métricas-de-desempenho-e-diagnóstico-de-rede)
   - [Speedup e Eficiência Paralela](#speedup-e-eficiência-paralela)
   - [Diagnóstico de Links, Retransmissões e Perda de Pacotes](#diagnóstico-de-links-retransmissões-e-perda-de-pacotes)
9. [Resolução de Problemas (Troubleshooting)](#9-resolução-de-problemas-troubleshooting)

---

## 1. Introdução e Filosofia do ESP-Hive

O **ESP-Hive** é um middleware de computação de alto desempenho (HPC - High Performance Computing) projetado especificamente para microcontroladores ESP32. Ele implementa uma camada inspirada no padrão da indústria **MPI (Message Passing Interface)**, combinada com a flexibilidade do **FreeRTOS** e a velocidade dos rádios proprietários da Espressif.

Tradicionalmente, programar sistemas distribuídos em microcontroladores exige lidar manualmente com sockets UDP crus, buffers de pacotes, perdas de sinal, timeouts e semáforos. O ESP-Hive abstrai toda essa complexidade:
- O programador foca apenas no algoritmo através de chamadas expressivas como `MPI_Send`, `MPI_Recv` e `MPI_Barrier`.
- O kernel do ESP-Hive gerencia no background o sequenciamento, confirmação com ACKs, retransmissões, buffers de mensagens fora de ordem e sincronização de barreira entre os nós.

---

## 2. Arquitetura de Hardware e Rede

### Nós Físicos vs Ranks Virtuais

Em um cluster ESP-Hive, existem dois conceitos fundamentais:
1. **Nó Físico (`Node`)**: É cada placa física ESP32 conectada à rede. Cada placa recebe um identificador único de `0` até `NUM_NODES - 1`. O **Node 0** é sempre o coordenador (**Master**), encarregado de orquestrar as barreiras e coordenar a inicialização.
2. **Rank Virtual (`Rank`)**: É cada tarefa lógica de processamento da aplicação, identificada de `0` até `TOTAL_APPLICATION_TASKS - 1`.

A atribuição de qual placa executa qual rank é feita pela fórmula estática de balanceamento por módulo:
$$\text{Node} = \text{Rank} \pmod{\text{NUM\_NODES}}$$

#### Exemplo Prático de Distribuição (4 Nós Físicos e 16 Ranks):
- **Node 0**: executa os ranks `0, 4, 8, 12`
- **Node 1**: executa os ranks `1, 5, 9, 13`
- **Node 2**: executa os ranks `2, 6, 10, 14`
- **Node 3**: executa os ranks `3, 7, 11, 15`

> **Vantagem:** Essa abordagem permite que seu cluster execute um número de tarefas lógicas muito maior que o número de placas físicas, aproveitando a capacidade preemptiva do FreeRTOS em ambos os núcleos do ESP32!

### Modos de Comunicação: ESP-NOW vs Wi-Fi UDP

O ESP-Hive suporta dois modos de comunicação física selecionáveis via configuração:

| Característica | ESP-NOW (`HIVE_MODE_ESPNOW`) | Wi-Fi UDP (`HIVE_MODE_WIFI`) |
| :--- | :--- | :--- |
| **Roteador Necessário?** | **Não** (comunicação direta rádio a rádio) | **Sim** (requer Ponto de Acesso Wi-Fi) |
| **Latência por Salto** | **Sub-milissegundo (~0.8 a 1.5 ms)** | 2 a 8 ms (dependente do tráfego do roteador) |
| **Overhead de Protocolo** | Mínimo (frames MAC de camada 2) | Médio (cabeçalhos IP e UDP) |
| **Canal de Frequência** | Fixo (configurável, padrão canal 2) | Determinado pelo roteador |
| **Recomendado para** | **Clusters dedicados de alto desempenho** | Redes com infraestrutura existente |

### Modo Standalone (Desenvolvimento em 1 Placa)

Se você configurar `NUM_NODES = 1`, o ESP-Hive entra automaticamente em **Modo Standalone**:
- Os rádios Wi-Fi e ESP-NOW **não** são ativados, economizando energia e acelerando o boot.
- Todos os ranks executam localmente na mesma placa via filas internas do FreeRTOS.
- Permite que você teste, depure e valide todo o seu código MPI em uma única bancada com apenas um cabo USB antes de implantar nas placas finais!

---

## 3. Instalação e Configuração no Arduino IDE

1. Baixe ou copie a pasta `ESP-Hive` para o diretório de bibliotecas do Arduino:
   - No Windows: `Documentos\Arduino\libraries\ESP-Hive`
   - No Linux: `~/Arduino/libraries/ESP-Hive`
   - No macOS: `~/Documents/Arduino/libraries/ESP-Hive`
2. Certifique-se de que o pacote de placas **esp32** da Espressif está instalado no *Boards Manager* do Arduino IDE (versão 2.x ou 3.x).
3. Abra o Arduino IDE. O menu **Arquivo -> Exemplos -> ESP-Hive** já conterá exemplos prontos para execução.

---

## 4. Estrutura Básica de um Programa ESP-Hive

Todo programa do ESP-Hive possui uma estrutura limpa e bem dividida em três partes:

```cpp
#include <Arduino.h>
#include <ESPHive.h>

// 1. Definições de Topologia do Cluster
#define THIS_NODE_ID     0       // 0 na placa Master; 1..N-1 nas placas Workers
#define NUM_NODES        2       // Quantidade de placas físicas
#define TOTAL_TASKS      4       // Quantidade total de ranks MPI

// 2. Função de Execução dos Ranks (ApplicationTask)
void ApplicationTask(void *parameter)
{
    int rank = MPI_Comm_rank();
    int size = MPI_Comm_size();

    // Sincronização inicial de todas as placas
    MPI_Barrier();

    if (rank == 0)
    {
        Serial.printf("[CLUSTER] Master iniciado com %d ranks totais!\n", size);
    }
    else
    {
        Serial.printf("[CLUSTER] Worker rank %d ativo no Node %d\n", rank, Hive.getNodeId());
    }

    // Sincronização final e término
    MPI_Barrier();
    MPI_Finalize();
}

// 3. Inicialização e Loop
void setup()
{
    Serial.begin(115200);
    delay(1000);

    HiveConfig cfg;
    cfg.nodeId     = THIS_NODE_ID;
    cfg.numNodes   = NUM_NODES;
    cfg.totalTasks = TOTAL_TASKS;
    cfg.commMode   = HIVE_MODE_ESPNOW; // Ou HIVE_MODE_WIFI

    if (!Hive.begin(cfg))
    {
        Serial.println("Erro crítico ao inicializar ESP-Hive!");
        while (true) delay(1000);
    }
}

void loop()
{
    // Mantém a supervisão de rede ativa no segundo plano
    Hive.update();
}
```

---

## 5. Referência Completa da API MPI

### `MPI_Comm_rank()`
Retorna o número de identificação único (rank) da tarefa chamadora.
```cpp
int rank = MPI_Comm_rank();
```
- **Retorno:** Inteiro de `0` até `MPI_Comm_size() - 1`. O rank `0` é convencionado como o processo Master. Retorna `-1` se chamado fora de uma task do cluster.

---

### `MPI_Comm_size()`
Retorna o número total de tarefas (ranks) distribuídas pelo cluster.
```cpp
int size = MPI_Comm_size();
```
- **Retorno:** O valor de `totalTasks` definido na inicialização.

---

### `MPI_Send()`
Envia dados de forma ponto-a-ponto e confiável para outro rank do cluster.
```cpp
bool MPI_Send(const void *buffer, int count, int destinationRank);
```
- **`buffer`**: Ponteiro para a memória que contém os dados a transmitir.
- **`count`**: Número de bytes a enviar (máximo de 128 bytes por pacote padrão).
- **`destinationRank`**: Rank do destinatário (`0` a `size - 1`).
- **Retorno:** `true` se a mensagem foi aceita e enfileirada com sucesso; `false` em caso de parâmetro inválido ou fila esgotada.
- **Confiabilidade:** Se o destino for em outro nó físico, o transmissor aguarda confirmação (ACK) por rádio e realiza retransmissões automáticas em caso de perda. Se o destino for local na mesma placa, a entrega é feita diretamente na fila FreeRTOS da task de destino.

---

### `MPI_Recv()`
Bloqueia a execução da tarefa atual até receber uma mensagem do remetente especificado.
```cpp
int MPI_Recv(void *buffer, int maxCount, int sourceRank);
```
- **`buffer`**: Ponteiro para o buffer que receberá os dados.
- **`maxCount`**: Tamanho máximo em bytes que o buffer pode armazenar.
- **`sourceRank`**: Rank do remetente esperado (`0` a `size - 1`), ou `MPI_ANY_SOURCE` (`-1`) para aceitar a primeira mensagem que chegar de qualquer remetente.
- **Retorno:** Quantidade de bytes efetivamente lidos no buffer, ou `-1` em caso de erro.
- **Buffer de Mensagens Fora de Ordem:** Se chegar uma mensagem de um remetente diferente do esperado, o ESP-Hive armazena-a em um buffer temporário de pendências e entrega-a quando o rank solicitar.

---

### `MPI_Barrier()`
Bloqueia todas as tarefas do cluster até que **todos** os ranks tenham alcançado esta chamada.
```cpp
MPI_Barrier();
```
- **Comportamento:** É uma primitiva de sincronização global. Os nós workers notificam o Master (Node 0). Quando todos os ranks informam sua chegada para a geração atual, o Master emite uma liberação global (Release).
- **Uso:** Essencial antes e depois de medições de tempo de benchmarks para garantir que nenhum nó iniciou a contagem antes dos outros terminarem a etapa anterior.

---

### `MPI_Bcast()`
Transmite dados de um rank raiz (root) para todos os outros ranks do cluster.
```cpp
bool MPI_Bcast(void *buffer, int count, int rootRank);
```
- **`buffer`**: No rank raiz, contém os dados a enviar. Nos demais ranks, é o buffer receptor.
- **`count`**: Tamanho dos dados em bytes.
- **`rootRank`**: O rank de onde se originam os dados (geralmente `0`).
- **Retorno:** `true` se a transmissão/recepção foi concluída com sucesso.

---

### `MPI_Finalize()` e `MPI_Finalized()`
Finaliza a execução do rank atual no ambiente de cluster.
```cpp
bool MPI_Finalize(void);
bool MPI_Finalized(void);
```
- **Descrição:** Marca a tarefa como concluída. Chamadas posteriores a `MPI_Send` ou `MPI_Recv` neste rank serão rejeitadas.

---

### `MPI_Wtime()` e `MPI_Wtime_micro()`
Funções de alta precisão para cronometragem e benchmark.
```cpp
double tSec = MPI_Wtime();        // Tempo em segundos (com fração decimal)
double tUs  = MPI_Wtime_micro();  // Tempo em microssegundos
```
- Utiliza o hardware de temporização de 64 bits de alta resolução do ESP32 (`esp_timer_get_time()`).

---

## 6. Padrões de Programação Distribuída

### Padrão 1: Master-Worker (Divisão de Fatias / Task Farming)
Ideal para problemas embaraçosamente paralelos (ex.: processamento de matrizes, renderização, simulações numéricas).
1. O Master divide o intervalo total $N$ em fatias uniformes.
2. Cada Worker recebe sua fatia via descritor de trabalho e computa em paralelo.

```cpp
// Divisão uniforme de N itens entre R ranks:
uint32_t baseChunk = N / size;
uint32_t remainder = N % size;

uint32_t myCount = baseChunk + (rank < remainder ? 1 : 0);
uint32_t myStart = rank * baseChunk + (rank < remainder ? rank : remainder);
```

### Padrão 2: Map-Reduce (Redução de Dados)
1. **Fase Map:** Cada rank calcula seu resultado intermediário localmente.
2. **Fase Reduce:** Todos os ranks enviam seus resultados para o Rank 0, que agrega (soma, média, mínimo, máximo).

```cpp
// Nos Workers:
MPI_Send(&myPartialResult, sizeof(myPartialResult), 0);

// No Master (Rank 0):
for (int src = 1; src < size; src++) {
    PartialData p;
    MPI_Recv(&p, sizeof(p), MPI_ANY_SOURCE);
    globalTotal += p.value;
}
```

### Padrão 3: Passagem de Bastão em Anel (Pipeline / Ring)
Cada nó envia dados para o próximo vizinho `(rank + 1) % size` e recebe do anterior `(rank - 1 + size) % size`.
- Excelente para filtros em pipeline, difusão em cadeia e medição de latência de salto a salto.

---

## 7. Boas Práticas de Sistemas Embarcados e FreeRTOS

### O Task Watchdog do ESP32 e o Fatiamento de CPU
O ESP32 possui um mecanismo de proteção chamado **TWDT (Task Watchdog Timer)**. As tarefas da aplicação rodam por padrão com prioridade 1 (mesma prioridade da tarefa `IDLE` do FreeRTOS).

> [!WARNING]
> Se uma tarefa de cálculo bruto (ex.: um loop `for` de milhões de iterações) rodar por vários segundos ininterruptos sem ceder a CPU, a tarefa `IDLE` não conseguirá resetar o watchdog, causando um **reset forçado da placa** com a mensagem:
> `Task watchdog got triggered. The following tasks did not reset the watchdog in time: APP_x`

#### Como resolver corretamente (Fatiamento de Amostras):
Insira pequenos pontos de cedência (`vTaskDelay(1)`) periodicamente no seu loop de cálculo:
```cpp
const uint32_t FATIA = 250000;
uint32_t processados = 0;

while (processados < total) {
    uint32_t bloco = min((uint32_t)FATIA, total - processados);
    
    // Processa o bloco intensivo
    for (uint32_t i = 0; i < bloco; i++) {
        // Cálculo pesado aqui...
    }
    
    processados += bloco;
    vTaskDelay(1); // Cede 1 tick para a task IDLE alimentar o Watchdog!
}
```

---

### Dimensionamento de Pilha (Stack) e Limite de Memória RAM

O ESP32 clássico possui cerca de 320 KB de SRAM utilizável por tarefas. Ao criar muitos ranks (ex.: 16 ou 32 ranks):
- O Rank 0 precisa de mais pilha porque lida com buffers e impressões complexas na Serial (`appStackSize` = 8192 bytes padrão).
- Os Ranks secundários executam laços mais simples (`appStackSizeSmall` = 4096 bytes padrão).

Você pode ajustar esses tamanhos no `HiveConfig`:
```cpp
HiveConfig cfg;
cfg.appStackSize      = 6144; // 6 KB para Rank 0
cfg.appStackSizeSmall = 3072; // 3 KB para Workers
cfg.debugStack        = true; // Ativa monitoramento de stack livre no loop()
```

---

### Envio de Cargas Maiores que 128 Bytes (Streaming de Mensagens)

O tamanho máximo padrão de uma mensagem individual é `HIVE_MESSAGE_PAYLOAD = 128` bytes (otimizado para caber com folga em um único frame de ESP-NOW sem fragmentação de rede).

Para enviar estruturas grandes ou vetores de dados:
```cpp
// Envio em blocos de 64 inteiros (128 bytes):
const int CHUNK_INTS = 64;
int dadosGrandes[1024]; // 4096 bytes

for (int offset = 0; offset < 1024; offset += CHUNK_INTS) {
    MPI_Send(&dadosGrandes[offset], CHUNK_INTS * sizeof(int), destinationRank);
}
```

---

## 8. Métricas de Desempenho e Diagnóstico de Rede

### Speedup e Eficiência Paralela

Ao avaliar o ganho de desempenho obtido pelo cluster, duas métricas universais devem ser calculadas:

1. **Speedup ($S_R$):** Razão entre o tempo de execução com 1 rank ($T_1$) e o tempo com $R$ ranks ($T_R$):
   $$S_R = \frac{T_1}{T_R}$$

2. **Eficiência Paralela ($E_R$):** Mede o aproveitamento real da capacidade de cada núcleo adicionado:
   $$E_R = \frac{S_R}{R} \times 100\%$$

Em problemas CPU-bound ideais com baixo volume de comunicação (como o benchmark de Monte Carlo), o Speedup se aproxima do valor linear ideal ($S_R \approx R$) e a eficiência se mantém acima de 85-95%.

---

### Diagnóstico de Links, Retransmissões e Perda de Pacotes

O ESP-Hive inclui ferramentas diagnósticas integradas de análise de tráfego de rádio:

```cpp
// Imprime estatísticas locais do nó:
Hive.printLocalStats();

// Coordena troca de snapshots com todos os vizinhos e gera tabela consolidada:
Hive.exchangeAndPrintStats();
```

#### Como Interpretar as Métricas:
- **`tentativas` vs `mensagens`**: Se `tentativas > mensagens`, ocorreram retransmissões no ar para compensar ruídos de RF. O protocolo garantiu a entrega sem perda de dados para a aplicação.
- **`dup` (Duplicatas)**: O nó vizinho recebeu o dado e enviou o ACK, mas o ACK se perdeu no ar. O transmissor retransmitiu e o receptor detectou a sequência repetida descartando-a automaticamente. Se `dup` estiver muito alto, pode indicar que `HIVE_DATA_ACK_TIMEOUT_MS` está muito baixo.
- **`filaCheia` (`rxQueueFull`)**: O rádio recebeu o pacote perfeitamente, mas a tarefa receptora demorou a ler com `MPI_Recv()` e a fila da task encheu. **Isso é uma perda de software, não de rádio.** Aumente `HIVE_QUEUE_LENGTH` ou faça a task consumir mais rápido.

---

## 9. Resolução de Problemas (Troubleshooting)

### 1. "Master fica preso em `Aguardando discovery...`"
- **Causa:** O Node 0 não está recebendo pacotes de broadcast das outras placas.
- **Soluções:**
  - Verifique se todas as placas foram compiladas com o **mesmo canal ESP-NOW** (`cfg.espnowChannel = 2`).
  - Verifique se cada placa física tem um `THIS_NODE_ID` **diferente** (Node 0, Node 1, Node 2...).
  - Certifique-se de que a distância entre as placas é razoável (coloque-as a pelo menos 30 cm de distância para evitar saturação do front-end de RF).

### 2. "Brownout Detector was triggered"
- **Causa:** Queda de tensão na linha de alimentação de 3.3V do ESP32 no momento de pico de transmissão de rádio (pode puxar picos de 300 mA a 500 mA).
- **Soluções:**
  - Conecte as placas em portas USB energizadas ou fontes externas com capacidade de pelo menos 1A por placa.
  - Adicione um capacitor eletrolítico de 100 µF a 470 µF entre os pinos 3V3 e GND da placa.

### 3. "Fila do rank cheia - sem ACK"
- **Causa:** O Rank 0 está enviando rajadas intensas de mensagens mais rápido do que o Worker consegue chamar `MPI_Recv()`.
- **Solução:** Aumente o tamanho da fila da task em `ESPHive_Config.h` (`HIVE_QUEUE_LENGTH`) ou introduza um `MPI_Barrier()` intermediário para compassar a rajada.

### 4. "Falha de alocação de memória / Reset por Heap"
- **Causa:** Configuração de muitos ranks (ex.: 32 ou 64 ranks) em placas ESP32 sem PSRAM.
- **Solução:** Reduza `totalTasks` ou utilize uma placa com PSRAM (WROVER), ou reduza `cfg.appStackSizeSmall`.

---

**ESP-Hive: Computação Paralela e Distribuída de Alta Performance ao Alcance de Todos os Desenvolvedores ESP32!**
