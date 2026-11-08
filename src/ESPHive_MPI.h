#ifndef ESP_HIVE_MPI_H
#define ESP_HIVE_MPI_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MPI_ANY_SOURCE (-1)

/**
 * @brief Retorna o identificador de rank único da tarefa atual no cluster.
 * @return Identificador do rank (0 a Comm_size - 1) ou -1 se fora de contexto.
 */
int MPI_Comm_rank(void);

/**
 * @brief Retorna o número total de tarefas (ranks) distribuídas no cluster.
 * @return Quantidade total de tarefas da aplicação.
 */
int MPI_Comm_size(void);

/**
 * @brief Retorna o tempo atual em segundos desde o boot (precisão de microssegundos).
 * @return Tempo decorrido em segundos como double.
 */
double MPI_Wtime(void);

/**
 * @brief Retorna o timestamp em microssegundos (esp_timer_get_time).
 * @return Tempo decorrido em microssegundos como double.
 */
double MPI_Wtime_micro(void);

/**
 * @brief Envia uma mensagem ponto-a-ponto de forma confiável para outro rank.
 * @param buffer Ponteiro para os dados a serem transmitidos.
 * @param count Número de bytes (máximo definido por HIVE_MESSAGE_PAYLOAD, padrão 128).
 * @param destinationRank Rank de destino (0 a Comm_size - 1).
 * @return true se enfileirado com sucesso, false caso contrário.
 */
bool MPI_Send(const void *buffer, int count, int destinationRank);

/**
 * @brief Recebe uma mensagem de um rank específico ou de qualquer rank.
 * @param buffer Ponteiro de destino para os bytes recebidos.
 * @param maxCount Tamanho máximo em bytes do buffer receptor.
 * @param sourceRank Rank do remetente esperado ou MPI_ANY_SOURCE (-1) para qualquer um.
 * @return Número de bytes efetivamente lidos no buffer, ou -1 em caso de erro.
 */
int MPI_Recv(void *buffer, int maxCount, int sourceRank);

/**
 * @brief Barreira de sincronização global distribuída.
 * Bloqueia a execução do rank atual até que todos os ranks do cluster
 * tenham chamado MPI_Barrier().
 */
void MPI_Barrier(void);

/**
 * @brief Transmite dados de um rank raiz para todos os outros ranks do cluster.
 * @param buffer Ponteiro para o buffer (dados de envio no root, destino nos demais).
 * @param count Tamanho dos dados em bytes.
 * @param rootRank Rank de onde os dados se originam.
 * @return true se concluído com sucesso, false em caso de falha.
 */
bool MPI_Bcast(void *buffer, int count, int rootRank);

/**
 * @brief Finaliza a participação do rank atual no ambiente de execução MPI.
 * @return true se finalizado com sucesso.
 */
bool MPI_Finalize(void);

/**
 * @brief Verifica se o rank atual já chamou MPI_Finalize().
 * @return true se já finalizado.
 */
bool MPI_Finalized(void);

#ifdef __cplusplus
}
#endif

#endif // ESP_HIVE_MPI_H
