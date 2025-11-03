/*******************************************************************************
 * DEMONSTRAÇÃO TÉCNICA (PoC): Change Data Capture (CDC)
 *******************************************************************************
 *
 * CONTEXTO:
 * Este script simula o monitoramento completo do ciclo de vida (INSERT, UPDATE,
 * DELETE) de um registro na tabela 'db_loja.cliente'.
 *
 * OBJETIVO:
 * 1. Validar a captura de todos os tipos de eventos (INSERT, UPDATE, DELETE).
 * 2. Entender o conceito de "Fila" (Consumir 'GET' vs. Espiar 'PEEK').
 * 3. Entender o que é o LSN (Log Sequence Number) um "marcador de página" para a fila
 *
 *******************************************************************************/

-- -----------------------------------------------------------------
-- PRÉ-REQUISITO: Checar o Nível do WAL
--
-- O resultado DEVE ser 'logical'.
--
SHOW wal_level;

-- -----------------------------------------------------------------
-- CONFIGURAÇÃO INICIAL (Limpeza e Preparação)
--
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'data_sync_slot';

-- -----------------------------------------------------------------
--
-- PASSO 1: Iniciar a Captura (REPLICATION SLOT)
--
-- Criamos o "consumidor" (slot). A partir daqui, o Postgres
-- começa a guardar as mudanças na "fila" deste slot.
--
SELECT pg_create_logical_replication_slot(
 'data_sync_slot',
 'test_decoding' -- Usamos 'test_decoding' pois ele gera saída em TEXTO, legível pela função
);

-- -----------------------------------------------------------------
--
-- PASSO 2: VERIFICAR O LSN INICIAL
--
-- Vamos checar o "marcador de página" (LSN) do nosso slot.
-- O 'restart_lsn' é o LSN (endereço) de onde o slot começará a ler.
--
SELECT slot_name, restart_lsn
FROM pg_replication_slots
WHERE slot_name = 'data_sync_slot';

-- -----------------------------------------------------------------
--
-- PASSO 3: SIMULAÇÃO DE TRANSAÇÕES (Enfileirando mudanças)
--
-- Cada COMMIT envia um novo conjunto de mensagens para a fila
-- do 'data_sync_slot'.

-- 3.1: Novo Cadastro (INSERT)
BEGIN;
INSERT INTO db_loja.cliente (id, nome, email, telefone, insert_date, is_delete)
VALUES (
 99999,
 'Cliente P/ Teste CDC',
 'cdc.teste@example.com',
 '(11) 98765-4321',
 NOW(),
 false
);
COMMIT;

-- 3.2: Atualização de Perfil (UPDATE)
BEGIN;
UPDATE db_loja.cliente
SET
 telefone = '(21) 99999-8888'
WHERE
 id = 99999;
COMMIT;

-- 3.3: Remoção do Cliente (DELETE)
BEGIN;
DELETE FROM db_loja.cliente
WHERE
 id = 99999;
COMMIT;

-- -----------------------------------------------------------------
--
-- PASSO 4: O CONCEITO DE LSN (Log Sequence Number) e FILA
--
-- O que é o LSN (Log Sequence Number)?
-- Pense no WAL (log de transações) como um livro-caixa "sequencial" onde cada transação é escrita.
-- O LSN é o número da página e linha (o endereço exato) de cada transação dentro desse livro. É um ponteiro único e sempre crescente.
--
-- O LSN e a Fila (Slot):
-- O slot (data_sync_slot) usa um LSN (restart_ls`) como marcador de página da sequencia de operações.
--
-- PEEK (Espiar): Apenas lê os dados. Não move o marcador.
-- GET (Pegar): Lê os dados e **move o marcador para frente (consumindo a fila).
--
-- -----------------------------------------------------------------

--
-- PASSO 4.1: ESPIANDO A FILA (PEEK)
--
SELECT lsn, data
FROM pg_logical_slot_peek_changes(
 'data_sync_slot',
 NULL,
 NULL
);
--
-- RESULTADO ESPERADO:
-- Você verá TODAS as 3 transações: INSERT, UPDATE e DELETE.
--

--
-- PASSO 4.2: VERIFICANDO O MARCADOR DO SLOT (Após PEEK)
--
SELECT slot_name, restart_lsn
FROM pg_replication_slots
WHERE slot_name = 'data_sync_slot';
--
-- RESULTADO ESPERADO:
-- O 'restart_lsn' será IDÊNTICO ao valor do PASSO 2.
-- 'PEEK' não moveu o marcador de página.
--

--
-- PASSO 4.3: CONSUMINDO A FILA (GET)
--
SELECT lsn, data
FROM pg_logical_slot_get_changes(
 'data_sync_slot',
 NULL,
 NULL
);
--
-- RESULTADO ESPERADO:
-- Você verá as 3 transações. No momento em que esta consulta termina,
-- o Postgres ATUALIZA o marcador de página do slot.
--

--
-- PASSO 4.4: VERIFICANDO O MARCADOR DO SLOT (Após GET)
--
SELECT slot_name, restart_lsn
FROM pg_replication_slots
WHERE slot_name = 'data_sync_slot';
--
-- RESULTADO ESPERADO:
-- SUCESSO! O 'restart_lsn' AVANÇOU para um novo endereço.
-- O marcador de página foi movido. A fila está "consumida".
--

--
-- PASSO 4.5: VERIFICANDO A FILA VAZIA (GET ou PEEK)
--
SELECT * FROM pg_logical_slot_get_changes(
 'data_sync_slot',
 NULL,
 NULL
);
--
-- RESULTADO ESPERADO:
-- NENHUM DADO. A fila está vazia.
--

-- -----------------------------------------------------------------
--
-- PASSO 5: (OBRIGATÓRIO) DESATIVAR E LIMPAR O AMBIENTE
--
-- IMPORTANTE: Se você não deletar o slot, o Postgres nunca
-- limpará os arquivos de log (WAL) associados a ele, e seu disco pode encher!
-- 
-- O slot guarda as operações (os arquivos de log WAL) para sempre, ou até acontecer uma destas duas coisas:
-- Elas serem consumidas por um pg_logical_slot_get_changes (que move o marcador).
-- O slot ser deletado (pg_drop_replication_slot).
--

-- 1. Desativa o slot de captura
SELECT pg_drop_replication_slot('data_sync_slot');

-- (Linha de DROP PUBLICATION REMOVIDA)


--
-- Fim da Demonstração
--