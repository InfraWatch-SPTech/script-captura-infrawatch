-- =============================================================
-- INFRAWATCH - SCRIPT DA NOVA MODELAGEM COM SEEDS
-- Banco: MySQL 8+
--
-- ATENCAO: DROP DATABASE apaga completamente o banco existente.
-- Use este script somente se puder recriar o banco do zero.
-- =============================================================

DROP DATABASE IF EXISTS InfraWatch;
CREATE DATABASE InfraWatch;
USE InfraWatch;

-- =============================================================
-- 1. CRIACAO DAS TABELAS
-- =============================================================

CREATE TABLE empresa (
    idEmpresa INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100) NOT NULL,
    cnpj VARCHAR(18),
    email VARCHAR(100),
    codigo CHAR(8) NOT NULL UNIQUE
);

CREATE TABLE usuario (
    idUsuario INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    telefone CHAR(9),
    senha VARCHAR(100) NOT NULL,
    fkEmpresa INT NOT NULL,

    CONSTRAINT fk_usuario_empresa
        FOREIGN KEY (fkEmpresa)
        REFERENCES empresa(idEmpresa)
);

CREATE TABLE nivel_acesso (
    idnivel_acesso INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(45) NOT NULL,
    descricao VARCHAR(80),
    fk_usuario INT NOT NULL,

    CONSTRAINT fk_nivel_acesso_usuario
        FOREIGN KEY (fk_usuario)
        REFERENCES usuario(idUsuario)
        ON DELETE CASCADE
);

CREATE TABLE permissao (
    idPermissao INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(50) NOT NULL UNIQUE,
    descricao VARCHAR(200)
);

CREATE TABLE permissoes_acesso (
    fkNivelAcesso INT NOT NULL,
    fkPermissao INT NOT NULL,

    CONSTRAINT pk_permissoes_acesso
        PRIMARY KEY (fkNivelAcesso, fkPermissao),

    CONSTRAINT fk_permissoes_acesso_nivel
        FOREIGN KEY (fkNivelAcesso)
        REFERENCES nivel_acesso(idnivel_acesso)
        ON DELETE CASCADE,

    CONSTRAINT fk_permissoes_acesso_permissao
        FOREIGN KEY (fkPermissao)
        REFERENCES permissao(idPermissao)
        ON DELETE CASCADE
);

CREATE TABLE endereco (
    idEndereco INT PRIMARY KEY AUTO_INCREMENT,
    logradouro VARCHAR(200) NOT NULL,
    numero VARCHAR(45) NOT NULL,
    bairro VARCHAR(45) NOT NULL,
    cidade VARCHAR(100) NOT NULL,
    estado CHAR(2) NOT NULL,
    cep CHAR(8) NOT NULL,
    complemento VARCHAR(45),
    fkEmpresa INT NOT NULL,

    CONSTRAINT fk_endereco_empresa
        FOREIGN KEY (fkEmpresa)
        REFERENCES empresa(idEmpresa)
        ON DELETE CASCADE
);

CREATE TABLE relatorio (
    idRelatorio INT PRIMARY KEY AUTO_INCREMENT,
    titulo VARCHAR(45) NOT NULL,
    descricao VARCHAR(500) NOT NULL,
    data_rel DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE equipamento (
    idEquipamento INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100) NOT NULL,
    tipo VARCHAR(50) NOT NULL,
    ip VARCHAR(25),
    status VARCHAR(20) NOT NULL,
    localizacao VARCHAR(150),
    descricao VARCHAR(200),
    fkEmpresa INT NOT NULL,
    fkEndereco INT,
    fkRelatorio INT,
    idMac varchar (150),
    valorDowntime FLOAT DEFAULT 100000.00,
    ultimaAtualizacao DATETIME,

    CONSTRAINT fk_equipamento_empresa
        FOREIGN KEY (fkEmpresa)
        REFERENCES empresa(idEmpresa),

    CONSTRAINT fk_equipamento_endereco
        FOREIGN KEY (fkEndereco)
        REFERENCES endereco(idEndereco)
        ON DELETE SET NULL,

    CONSTRAINT fk_equipamento_relatorio
        FOREIGN KEY (fkRelatorio)
        REFERENCES relatorio(idRelatorio)
        ON DELETE SET NULL
);

CREATE TABLE equipe (
	idEquipe INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100),
    descricao VARCHAR(200)
);

CREATE TABLE usuario_equipamento (
	fkUsuario INT,
    fkEquipamento INT,
    fkEquipe INT,
    
    CONSTRAINT pk_usuario_servidor
		PRIMARY KEY (fkUsuario, fkEquipamento),
        
	CONSTRAINT fk_usuario_ue
		FOREIGN KEY (fkUsuario)
        REFERENCES usuario(idUsuario),
        
	CONSTRAINT fk_equip_ue
		FOREIGN KEY (fkEquipamento)
        REFERENCES equipamento(idEquipamento),
        
	CONSTRAINT fk_equipe_ue
		FOREIGN KEY (fkEquipe)
        REFERENCES equipe(idEquipe)
);

CREATE TABLE componente (
    idComponente INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100) NOT NULL,
    tipo VARCHAR(50) NOT NULL,
    codigo VARCHAR(200),
    medida VARCHAR(50),
    descricao VARCHAR(200)
);

CREATE TABLE parametro_alerta (
    nomeMetrica VARCHAR(50) NOT NULL,
    limite_atencao DECIMAL(10,2) NOT NULL,
    limite_critico DECIMAL(10,2) NOT NULL,
    unidade VARCHAR(20) NOT NULL DEFAULT '%',
    ativo TINYINT(1) NOT NULL DEFAULT 1,
    fkEquipamento INT NOT NULL,
    fkComponente INT NOT NULL,

    CONSTRAINT pk_parametro_alerta
        PRIMARY KEY (fkEquipamento, fkComponente),

    CONSTRAINT fk_parametro_alerta_equipamento
        FOREIGN KEY (fkEquipamento)
        REFERENCES equipamento(idEquipamento)
        ON DELETE CASCADE,

    CONSTRAINT fk_parametro_alerta_componente
        FOREIGN KEY (fkComponente)
        REFERENCES componente(idComponente)
);

-- =============================================================
-- 2. SEEDS DE EMPRESAS
-- A empresa de codigo NOVA0001 fica sem usuarios para permitir
-- o teste da regra: primeiro usuario da empresa recebe nivel Admin.
-- =============================================================

INSERT INTO empresa (nome, cnpj, email, codigo) VALUES
('InfraWatch', '12.345.555/0001-90', 'contato@infrawatch.com', 'X678JNLL'),
('TechShield Ltda', '12.345.678/0001-90', 'contato@TechShield.com', 'X678JNSZ'),
('XPTO Brasil', '98.765.432/0001-10', 'suporte@xpto.com', 'K492MLQX'),
('Empresa para Teste', NULL, 'teste@empresa.com', 'NOVA0001');


-- =============================================================
-- 3. SEEDS DE USUARIOS
-- Senhas simples somente para ambiente academico/de teste.
-- Em producao, as senhas devem ser armazenadas com hash.
-- =============================================================

INSERT INTO usuario
(nome, email, telefone, senha, fkEmpresa)
VALUES
('Root InfraWatch', 'admin@infrawatch.com', '999999999', 'admin123', 1),
('Mariana', 'mariana@techshield.com', '988888888', 'admin123', 2),
('Analista Geral', 'analista@TechShield.com', '977777777', 'usuario123', 2),
('Analista de Alertas', 'alertas@TechShield.com', '966666666', 'usuario123', 2),
('Auditor XPTO', 'auditor@xpto.com', '955555555', 'usuario123', 3);

-- Cada usuario recebe um nivel de acesso.
INSERT INTO nivel_acesso (nome, descricao, fk_usuario) VALUES
('Root', 'Acesso completo ao sistema', 1),
('Administrador', 'Administra a empresa e seus usuarios', 2),
('Analista geral', 'Visualiza as principais dashboards', 3),
('Analista de alertas', 'Visualiza e gerencia alertas', 4),
('Auditor', 'Acesso somente para consulta', 5);

-- =============================================================
-- 3.1. SEEDS DE EQUIPES E VINCULOS USUARIO-EQUIPE
-- Regra: usuarios de nivel administrativo (Root/Administrador)
-- podem cuidar de varias equipes; usuarios comuns pertencem a
-- apenas uma equipe.
-- =============================================================

INSERT INTO equipe (nome, descricao) VALUES
('Equipe de Infraestrutura', 'Cuida dos servidores da empresa'),
('Equipe de Rede', 'Monitora switches e conectividade'),
('Equipe de Alertas', 'Trata e configura os alertas'),
('Equipe de Auditoria', 'Acompanha relatorios e auditoria');

-- =============================================================
-- 4. SEEDS DE PERMISSOES ESPECIFICAS
-- =============================================================

INSERT INTO permissao (nome, descricao) VALUES
('DASHBOARD_GERAL_VISUALIZAR', 'Permite visualizar a dashboard geral'),
('EQUIPAMENTOS_VISUALIZAR', 'Permite visualizar equipamentos'),
('EQUIPAMENTOS_CADASTRAR', 'Permite cadastrar equipamentos'),
('EQUIPAMENTOS_EDITAR', 'Permite editar equipamentos'),
('EQUIPAMENTOS_EXCLUIR', 'Permite excluir equipamentos'),
('ALERTAS_VISUALIZAR', 'Permite visualizar alertas'),
('ALERTAS_CONFIGURAR', 'Permite configurar os limites dos alertas'),
('RELATORIOS_VISUALIZAR', 'Permite visualizar relatorios'),
('RELATORIOS_GERENCIAR', 'Permite criar, editar e excluir relatorios'),
('USUARIOS_VISUALIZAR', 'Permite visualizar usuarios da empresa'),
('USUARIOS_GERENCIAR', 'Permite administrar usuarios e niveis de acesso');

-- Root: todas as permissoes.
INSERT INTO permissoes_acesso (fkNivelAcesso, fkPermissao)
SELECT 1, idPermissao
FROM permissao;

-- Administrador: todas as permissoes.
INSERT INTO permissoes_acesso (fkNivelAcesso, fkPermissao)
SELECT 2, idPermissao
FROM permissao;

-- Analista geral: apenas visualizacao.
INSERT INTO permissoes_acesso (fkNivelAcesso, fkPermissao) VALUES
(3, 1),
(3, 2),
(3, 6),
(3, 8);

-- Analista de alertas: dashboard geral e gestao de alertas.
INSERT INTO permissoes_acesso (fkNivelAcesso, fkPermissao) VALUES
(4, 1),
(4, 6),
(4, 7);

-- Auditor: apenas consulta de equipamentos, alertas e relatorios.
INSERT INTO permissoes_acesso (fkNivelAcesso, fkPermissao) VALUES
(5, 1),
(5, 2),
(5, 6),
(5, 8);

-- =============================================================
-- 5. SEEDS DE ENDERECOS
-- =============================================================

INSERT INTO endereco
(logradouro, numero, bairro, cidade, estado, cep, complemento, fkEmpresa)
VALUES
('Avenida Paulista', '1000', 'Bela Vista', 'Sao Paulo', 'SP', '01310100', 'Datacenter A', 2),
('Rua das Flores', '250', 'Centro', 'Campinas', 'SP', '13010000', 'Sala de servidores', 2),
('Avenida Brasil', '500', 'Centro', 'Rio de Janeiro', 'RJ', '20040002', 'Datacenter principal', 3);

-- =============================================================
-- 6. SEEDS DE RELATORIOS
-- Na modelagem recebida, equipamento possui a FK de relatorio.
-- Por isso, os relatorios sao inseridos antes dos equipamentos.
-- =============================================================

INSERT INTO relatorio (titulo, descricao, data_rel) VALUES
('Desempenho principal', 'Relatorio de CPU, RAM e disco do servidor principal.', '2026-09-25 10:00:00'),
('Rotina de backup', 'Relatorio de verificacao do servidor de backup.', '2026-09-26 11:30:00'),
('Disponibilidade da rede', 'Relatorio de funcionamento do switch central.', '2026-09-27 14:00:00'),
('Servidor web XPTO', 'Relatorio do servidor web em estado de atencao.', '2026-09-28 16:45:00');

-- =============================================================
-- 7. SEEDS DE EQUIPAMENTOS
-- =============================================================

INSERT INTO equipamento
(nome, tipo, ip, status, localizacao, descricao,
 fkEmpresa, fkEndereco, fkRelatorio)
VALUES
('Servidor Principal', 'Servidor', '192.168.1.10', 'Online', 'Datacenter A', 'Servidor principal da empresa', 2, 1, 1),
('Servidor de Backup', 'Servidor', '192.168.1.20', 'Online', 'Datacenter A', 'Servidor de copias de seguranca', 2, 1, 2),
('Switch Central', 'Switch', '192.168.1.30', 'Online', 'Sala de servidores', 'Switch central da rede', 2, 2, 3),
('Servidor Web XPTO', 'Servidor', '192.168.2.10', 'Atencao', 'Datacenter principal', 'Servidor web da XPTO Brasil', 3, 3, 4);

-- Servidores extras da TechShield (empresa 2) usados pelo simulador-captura.py.
-- Junto com os ids 1 e 2, totalizam 5 servidores para a simulacao.
INSERT INTO equipamento
(idEquipamento, nome, tipo, ip, status, localizacao, descricao,
 fkEmpresa, fkEndereco, fkRelatorio)
VALUES
(5, 'Servidor Simulado 01', 'Servidor', '192.168.1.41', 'Online', 'Datacenter A', 'Servidor usado na simulacao', 2, 1, 1),
(6, 'Servidor Simulado 02', 'Servidor', '192.168.1.42', 'Online', 'Datacenter A', 'Servidor usado na simulacao', 2, 1, 2),
(7, 'Servidor Simulado 03', 'Servidor', '192.168.1.43', 'Online', 'Sala de servidores', 'Servidor usado na simulacao', 2, 2, 3);

-- =============================================================
-- 7.1. SEEDS DE USUARIO_EQUIPAMENTO
-- Tabela de associacao entre usuario, equipe e equipamento.
-- Regras adotadas (coerentes com os seeds de usuario_equipe):
--   * A PK e (fkUsuario, fkEquipamento), logo cada par usuario/equipamento
--     aparece uma unica vez, com uma unica equipe responsavel.
--   * fkEquipe sempre corresponde a uma equipe da qual o usuario ja
--     participa em usuario_equipe.
--   * Usuarios comuns atendem equipamentos da sua empresa; o Root, por
--     ser administrador global, atende a todos os equipamentos.
-- Equipe 1 = Infraestrutura (servidores), 2 = Rede (switch),
-- 3 = Alertas, 4 = Auditoria.
-- =============================================================

INSERT INTO usuario_equipamento (fkUsuario, fkEquipamento, fkEquipe) VALUES
-- Root (usuario 1): participa de todas as equipes e cobre todos os equipamentos.
(1, 1, 1), -- Servidor Principal        -> Infraestrutura
(1, 2, 1), -- Servidor de Backup        -> Infraestrutura
(1, 3, 2), -- Switch Central            -> Rede
(1, 4, 4), -- Servidor Web XPTO         -> Auditoria
(1, 5, 1), -- Servidor Simulado 01      -> Infraestrutura
(1, 6, 1), -- Servidor Simulado 02      -> Infraestrutura
(1, 7, 1), -- Servidor Simulado 03      -> Infraestrutura
-- Mariana (usuario 2, administradora TechShield): equipes 1 e 2.
(2, 1, 1),
(2, 2, 1),
(2, 3, 2),
(2, 5, 1),
(2, 6, 1),
(2, 7, 1),
-- Analista Geral (usuario 3): apenas equipe de Infraestrutura.
(3, 1, 1),
(3, 2, 1),
(3, 5, 1),
(3, 6, 1),
(3, 7, 1),
-- Analista de Alertas (usuario 4): apenas equipe de Alertas.
(4, 1, 3),
(4, 2, 3),
(4, 3, 3),
(4, 5, 3),
(4, 6, 3),
(4, 7, 3),
-- Auditor XPTO (usuario 5): apenas equipe de Auditoria, equipamento da empresa 3.
(5, 4, 4);

-- =============================================================
-- 8. SEEDS DE COMPONENTES
-- Os componentes funcionam como um catalogo. A associacao com
-- os equipamentos acontece por meio da tabela parametro_alerta.
-- =============================================================

INSERT INTO componente (nome, tipo, descricao, codigo, medida) VALUES
('Processador', 'CPU', 'Uso do processador', 'psutil.cpu_percent(interval=1)', '%'),
('Memoria RAM', 'RAM', 'Uso da memoria principal', 'psutil.virtual_memory().percent', '%'),
('Disco', 'ARMAZENAMENTO', 'Uso do armazenamento', 'psutil.disk_usage("/").percent', '%'),
('Temperatura', 'TEMPERATURA', 'Temperatura interna do equipamento', 'código_psutil', '°'),
('Rede', 'REDE', 'Uso da rede do equipamento', 'código_psutil', 'kb'),
('Processos', 'PROCESSOS', 'Todos os processos ativos no equipamento', '', '');

-- =============================================================
-- 9. SEEDS DE PARAMETROS DE ALERTA
-- =============================================================

INSERT INTO parametro_alerta
(nomeMetrica, limite_atencao, limite_critico, unidade,
 ativo, fkEquipamento, fkComponente)
VALUES
('USO_CPU', 70.00, 90.00, '%', 1, 1, 1),
('USO_RAM', 75.00, 90.00, '%', 1, 1, 2),
('USO_DISCO', 80.00, 95.00, '%', 1, 1, 3),
('TEMPERATURA_CPU', 65.00, 80.00, 'C', 1, 1, 4),
('USO_CPU', 70.00, 90.00, '%', 1, 2, 1),
('USO_RAM', 75.00, 90.00, '%', 1, 2, 2),
('USO_DISCO', 80.00, 95.00, '%', 1, 2, 3),
('USO_REDE', 70.00, 90.00, '%', 1, 3, 5),
('USO_CPU', 65.00, 85.00, '%', 1, 4, 1),
('USO_RAM', 70.00, 90.00, '%', 1, 4, 2),
('USO_DISCO', 75.00, 90.00, '%', 1, 4, 3),
('PROCESSOS_ATIVOS', 65.00, 80.00, 'Dict',1,1,6);

-- Parametros dos servidores simulados (ids 5, 6 e 7) da TechShield.
INSERT INTO parametro_alerta
(nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente)
VALUES
('USO_CPU', 70.00, 90.00, '%', 1, 5, 1),
('USO_RAM', 75.00, 90.00, '%', 1, 5, 2),
('USO_DISCO', 80.00, 95.00, '%', 1, 5, 3),
('TEMPERATURA_CPU', 65.00, 80.00, 'C', 1, 5, 4),
('PROCESSOS_ATIVOS', 65.00, 80.00, 'Dict', 1, 5, 6),
('USO_CPU', 70.00, 90.00, '%', 1, 6, 1),
('USO_RAM', 75.00, 90.00, '%', 1, 6, 2),
('USO_DISCO', 80.00, 95.00, '%', 1, 6, 3),
('TEMPERATURA_CPU', 65.00, 80.00, 'C', 1, 6, 4),
('PROCESSOS_ATIVOS', 65.00, 80.00, 'Dict', 1, 6, 6),
('USO_CPU', 70.00, 90.00, '%', 1, 7, 1),
('USO_RAM', 75.00, 90.00, '%', 1, 7, 2),
('USO_DISCO', 80.00, 95.00, '%', 1, 7, 3),
('TEMPERATURA_CPU', 65.00, 80.00, 'C', 1, 7, 4),
('PROCESSOS_ATIVOS', 65.00, 80.00, 'Dict', 1, 7, 6);

-- =============================================================
-- 10. USUÁRIOS PARA APLICAÇÕES
-- =============================================================

-- PARA DESCOBRIR EQUIPE DOS SERVIDORES
SELECT idEmpresa, nome FROM empresa WHERE nome = 'TechShield LTDA';
SELECT
	equipa.idEquipamento,
	equipa.nome,
    equipa.valorDowntime,
    equipe.nome
FROM empresa AS e
JOIN equipamento AS equipa
	ON equipa.fkEmpresa = e.idEmpresa
JOIN usuario_equipamento AS ue
	ON ue.fkEquipamento = equipa.idEquipamento
JOIN equipe
	ON equipe.idEquipe = ue.fkEquipe
WHERE e.idEmpresa = 2
GROUP BY equipa.idEquipamento, equipe.nome;


-- PARA DESCOBRIR PARÂMETRO SERVIDORES
SELECT
	equipa.idEquipamento AS id,
	equipa.nome AS nome,
    equipa.valorDowntime AS valorDowntime,
    p.limite_atencao AS atencao,
    p.limite_critico AS critico,
    c.nome AS componente
FROM empresa AS e
JOIN equipamento AS equipa
	ON equipa.fkEmpresa = e.idEmpresa
JOIN parametro_alerta AS p
	ON p.fkEquipamento = equipa.idEquipamento
JOIN componente AS c
	ON p.fkComponente = c.idComponente
WHERE e.idEmpresa = 2;


-- SCRIPT JAVA JIRA USER --
DROP USER IF EXISTS 'infra_watch_java_jira'@'%';
CREATE USER 'infra_watch_java_jira'@'%' IDENTIFIED BY 'Urubu100';
GRANT SELECT ON InfraWatch.* TO 'infra_watch_java_jira'@'%';
FLUSH PRIVILEGES;

DROP USER IF EXISTS 'infra_watch_captura'@'%';
CREATE USER 'infra_watch_captura'@'%' IDENTIFIED BY 'Urubu100';
GRANT all privileges  ON InfraWatch.* TO 'infra_watch_captura'@'%';
FLUSH PRIVILEGES;