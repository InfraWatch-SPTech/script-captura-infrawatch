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
    empresa_idEmpresa INT NOT NULL,

    CONSTRAINT fk_endereco_empresa
        FOREIGN KEY (empresa_idEmpresa)
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
    endereco_idEndereco INT,
    relatorio_idRelatorio INT,

    CONSTRAINT fk_equipamento_empresa
        FOREIGN KEY (fkEmpresa)
        REFERENCES empresa(idEmpresa),

    CONSTRAINT fk_equipamento_endereco
        FOREIGN KEY (endereco_idEndereco)
        REFERENCES endereco(idEndereco)
        ON DELETE SET NULL,

    CONSTRAINT fk_equipamento_relatorio
        FOREIGN KEY (relatorio_idRelatorio)
        REFERENCES relatorio(idRelatorio)
        ON DELETE SET NULL
);

CREATE TABLE componente (
    idComponente INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100) NOT NULL,
    tipo VARCHAR(50) NOT NULL,
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
('Bananinha Ltda', '12.345.678/0001-90', 'contato@bananinha.com', 'X678JNSZ'),
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
('Admin Bananinha', 'admin@bananinha.com', '988888888', 'admin123', 2),
('Analista Geral', 'analista@bananinha.com', '977777777', 'usuario123', 2),
('Analista de Alertas', 'alertas@bananinha.com', '966666666', 'usuario123', 2),
('Auditor XPTO', 'auditor@xpto.com', '955555555', 'usuario123', 3);

-- Cada usuario recebe um nivel de acesso.
INSERT INTO nivel_acesso (nome, descricao, fk_usuario) VALUES
('Root', 'Acesso completo ao sistema', 1),
('Administrador', 'Administra a empresa e seus usuarios', 2),
('Analista geral', 'Visualiza as principais dashboards', 3),
('Analista de alertas', 'Visualiza e gerencia alertas', 4),
('Auditor', 'Acesso somente para consulta', 5);

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
(logradouro, numero, bairro, cidade, estado, cep, complemento, empresa_idEmpresa)
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
 fkEmpresa, endereco_idEndereco, relatorio_idRelatorio)
VALUES
('Servidor Principal', 'Servidor', '192.168.1.10', 'Online', 'Datacenter A', 'Servidor principal da empresa', 2, 1, 1),
('Servidor de Backup', 'Servidor', '192.168.1.20', 'Online', 'Datacenter A', 'Servidor de copias de seguranca', 2, 1, 2),
('Switch Central', 'Switch', '192.168.1.30', 'Online', 'Sala de servidores', 'Switch central da rede', 2, 2, 3),
('Servidor Web XPTO', 'Servidor', '192.168.2.10', 'Atencao', 'Datacenter principal', 'Servidor web da XPTO Brasil', 3, 3, 4);

-- =============================================================
-- 8. SEEDS DE COMPONENTES
-- Os componentes funcionam como um catalogo. A associacao com
-- os equipamentos acontece por meio da tabela parametro_alerta.
-- =============================================================

INSERT INTO componente (nome, tipo, descricao) VALUES
('Processador', 'CPU', 'Uso do processador'),
('Memoria RAM', 'RAM', 'Uso da memoria principal'),
('Disco', 'ARMAZENAMENTO', 'Uso do armazenamento'),
('Temperatura', 'TEMPERATURA', 'Temperatura interna do equipamento'),
('Rede', 'REDE', 'Uso da rede do equipamento');

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
('USO_DISCO', 75.00, 90.00, '%', 1, 4, 3);

-- =============================================================
-- 10. CONSULTAS PARA CONFERENCIA
-- =============================================================

-- Usuarios, niveis e permissoes.
SELECT
    u.idUsuario,
    u.nome AS usuario,
    e.nome AS empresa,
    na.nome AS nivel_acesso,
    p.nome AS permissao
FROM usuario u
JOIN empresa e
    ON e.idEmpresa = u.fkEmpresa
LEFT JOIN nivel_acesso na
    ON na.fk_usuario = u.idUsuario
LEFT JOIN permissoes_acesso pa
    ON pa.fkNivelAcesso = na.idnivel_acesso
LEFT JOIN permissao p
    ON p.idPermissao = pa.fkPermissao
ORDER BY u.idUsuario, p.idPermissao;

-- Equipamentos, componentes e limites de alerta.
SELECT
    eq.nome AS equipamento,
    c.nome AS componente,
    pa.nomeMetrica,
    pa.limite_atencao,
    pa.limite_critico,
    pa.unidade,
    pa.ativo
FROM parametro_alerta pa
JOIN equipamento eq
    ON eq.idEquipamento = pa.fkEquipamento
JOIN componente c
    ON c.idComponente = pa.fkComponente
ORDER BY eq.idEquipamento, c.idComponente;

-- SCRIPT JAVA JIRA USER --
DROP USER IF EXISTS 'infra_watch_java_jira'@'%';
CREATE USER 'infra_watch_java_jira'@'%' IDENTIFIED BY 'Urubu100';
GRANT SELECT ON InfraWatch.* TO 'infra_watch_java_jira'@'%';
FLUSH PRIVILEGES;

DROP USER IF EXISTS 'infra_watch_captura'@'%';
CREATE USER 'infra_watch_captura'@'%' IDENTIFIED BY 'Urubu100';
GRANT SELECT ON InfraWatch.* TO 'infra_watch_captura'@'%';
FLUSH PRIVILEGES;






