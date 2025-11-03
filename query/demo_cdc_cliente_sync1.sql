/*******************************************************************************
 * DEMONSTRAÇÃO TÉCNICA (PoC): Change Data Capture (CDC)
 *******************************************************************************
 *
 * CONTEXTO:
 * Este script simula o monitoramento da tabela 'db_loja.cliente' para
 * alimentar um pipeline de dados em tempo real.
 *
 * OBJETIVO:
 * Validar a captura de eventos (INSERTs, UPDATEs) que seriam enviados
 * a um sistema externo (ex: CRM, Data Warehouse).
 *
 * EXECUÇÃO:
 * Este script é interativo. Execute cada "PASSO" separadamente e
 * observe os resultados antes de prosseguir.
 *
 *******************************************************************************/

-- -----------------------------------------------------------------
-- CONFIGURAÇÃO INICIAL (Limpeza e Preparação)
--
-- Garante que o ambiente de teste esteja limpo antes de começar.

-- (Limpa o slot de replicação se ele existir de uma execução anterior)
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'data_sync_slot';

-- (Limpa a publicação de dados se ela existir)
DROP PUBLICATION IF EXISTS data_sync_pub;

--
-- PASSO 1: Definir a Fonte de Dados (PUBLICATION)
--
-- Criamos uma "Publicação" que age como um "canal".
-- Aqui, definimos que este canal *somente* transmitirá
-- mudanças da tabela 'db_loja.cliente'.
--
CREATE PUBLICATION data_sync_pub FOR TABLE db_loja.cliente;

-- -----------------------------------------------------------------
--
--
-- PASSO 2: Iniciar a Captura (REPLICATION SLOT)
--
-- Agora, criamos um "consumidor" (slot) para o canal.
-- A partir deste ponto, o PostgreSQL *reterá* todos os logs de
-- transação (WAL) para a tabela 'cliente', até que este
-- slot confirme que os consumiu.
--
SELECT pg_create_logical_replication_slot(
 'data_sync_slot', -- Nome do slot consumidor
 'pgoutput' -- Plugin de decodificação padrão
);

-- -----------------------------------------------------------------
--
--
-- PASSO 3: SIMULAÇÃO DE TRANSAÇÃO (Novo Cadastro - INSERT)
--
-- Um novo cliente (ID 99999) se cadastra no sistema.
--
BEGIN;
INSERT INTO db_loja.cliente (id, nome, email, telefone, insert_date, is_delete)
VALUES (
 99999,
 'Cliente Novo P/ Teste CDC',
 'cdc.novo@example.com',
 '(11) 98765-4321',
 NOW(),
 false
);
COMMIT;

-- -----------------------------------------------------------------
--
-- PASSO 4: VERIFICAR A CAPTURA (Consumir o INSERT)
--
-- Consultamos o slot para "ler" as mudanças pendentes.
--
SELECT * FROM pg_logical_slot_get_changes(
 'data_sync_slot',
 NULL,
 NULL,
 'publication_names',
 'data_sync_pub'
);
--
-- RESULTADO ESPERADO:
-- Você verá o 'BEGIN', o 'COMMIT' e, no meio, a linha de 'data'
-- com o "INSERT" e todos os dados do cliente (ID 99999).
-- O pipeline externo leria isso para criar o novo registro no CRM.
--

-- -----------------------------------------------------------------
--
-- PASSO 5: SIMULAÇÃO DE TRANSAÇÃO (Atualização de Perfil - UPDATE)
--
-- O mesmo cliente (ID 99999) atualiza seu número de telefone.
--
BEGIN;
UPDATE db_loja.cliente
SET
 telefone = '(21) 99999-8888'
WHERE
 id = 99999;
COMMIT;

-- -----------------------------------------------------------------
--
-- PASSO 6: VERIFICAR A CAPTURA (Consumir o UPDATE)
--
-- Lemos o slot novamente.
--
SELECT * FROM pg_logical_slot_get_changes(
 'data_sync_slot',
 NULL,
 NULL,
 'publication_names',
 'data_sync_pub'
);
--
-- RESULTADO ESPERADO:
-- Você verá *APENAS* a transação de UPDATE. O INSERT do Passo 3
-- já foi "consumido" e saiu da fila.
-- O CRM usaria este evento para atualizar o registro existente.
--

-- -----------------------------------------------------------------
--
-- PASSO 7: SIMULAÇÃO DE TRANSAÇÃO (Anonimização LGPD/GDPR - UPDATE)
--
-- O cliente (ID 99999) solicita a exclusão de sua conta.
-- Em vez de um 'DELETE', a política da empresa é um "soft delete"
-- (anonimizando os dados) para manter o histórico de pedidos.
--
BEGIN;
UPDATE db_loja.cliente
SET
 nome = 'Cliente Anonimizado',
 email = 'anonimizado@' || id::text || '.invalid',
 telefone = NULL,
 is_delete = true
WHERE
 id = 99999;
COMMIT;

-- -----------------------------------------------------------------
--
--
-- PASSO 8: VERIFICAR A CAPTURA (Consumir a Anonimização)
--
SELECT * FROM pg_logical_slot_get_changes(
 'data_sync_slot',
 NULL,
 NULL,
 'publication_names',
 'data_sync_pub'
);
--
-- RESULTADO ESPERADO:
-- Você verá a transação de UPDATE final, com os novos
-- dados anonimizados, pronta para ser replicada no sistema externo.
--

-- -----------------------------------------------------------------
-- PASSO 9: (OBRIGATÓRIO) DESATIVAR E LIMPAR O AMBIENTE
--
-- IMPORTANTE: Slots de replicação ativos impedem o PostgreSQL
-- de limpar arquivos de log (WAL). Se um slot não for deletado
-- (ou consumido regularmente), ele pode encher o disco do servidor.

-- 1. Desativa o slot de captura
SELECT pg_drop_replication_slot('data_sync_slot');

-- 2. Remove a publicação
DROP PUBLICATION data_sync_pub;

-- 3. Limpa o registro de teste do banco
DELETE FROM db_loja.cliente WHERE id = 99999;

--
-- Fim da Demonstração
--