DROP DATABASE IF EXISTS InfraWatch;

CREATE DATABASE InfraWatch;
USE InfraWatch;

CREATE TABLE empresa (
	id INT PRIMARY KEY AUTO_INCREMENT,
    razao_social VARCHAR(100),
    cnpj CHAR(14)
);

CREATE TABLE usuario (
    id INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    senha VARCHAR(100) NOT NULL,
    fk_empresa INT,
    FOREIGN KEY (fk_empresa) REFERENCES empresa(id)
);

CREATE TABLE equipamento (
	id INT PRIMARY KEY AUTO_INCREMENT,
    codigo VARCHAR(100) UNIQUE NOT NULL,
    nome VARCHAR(50),
	fk_empresa INT,
    FOREIGN KEY (fk_empresa) REFERENCES empresa(id)
);


CREATE TABLE componente (
    id INT PRIMARY KEY AUTO_INCREMENT,
    codigo VARCHAR(200) NOT NULL,
    nome VARCHAR(50),
    medida VARCHAR(50)
);

CREATE TABLE equipamento_componente (
	fk_componente INT,
    fk_equipamento INT,
    CONSTRAINT PRIMARY KEY (fk_componente, fk_equipamento),
    FOREIGN KEY (fk_equipamento) REFERENCES equipamento(id),
    FOREIGN KEY (fk_componente) REFERENCES componente(id)
);

INSERT INTO empresa (razao_social, cnpj) VALUES
('InfraWatch', '12345555000190'),
('Bananinha Ltda', '12345678000190'),
('Xpto Brasil', '98765432000110');

INSERT INTO usuario (nome, email, senha, fk_empresa) VALUES 
-- InfraWatch
('ana.reis', 'ana.reis@infrawatch.com', '1234', 1),
('gustavo.costa', 'gustavo.costa@infrawatch.com', '123456', 1),
('karina.cupola', 'karina.cupola@infrawatch.com', '123456', 1),
-- Xpto Brasil
('matheus.paula', 'matheus.paula@xpto.com', '123456', 2),
('pedro.barros', 'pedro.barros@xpto.com', '123456', 2),
-- Itau Unibank
('pedro.pereira', 'pedro.pereira@itau.com', '123456', 3);

INSERT INTO equipamento (codigo, nome, fk_empresa) VALUES
("010101", 'Servidor', 1),
("121212", 'Switch', 2),
("020202", 'Roteador', 3);

INSERT INTO componente (codigo, nome, medida) VALUES
('psutil.cpu_percent(interval=2)', 'cpu', '%'),
('psutil.virtual_memory().percent', 'ram', '%'),
('psutil.disk_usage("/").percent', 'disco', '%');

INSERT INTO equipamento_componente (fk_componente, fk_equipamento) VALUES
(1, 1),
(2, 3),
(3, 2),
(1, 2),
(2, 2),
(3, 1),
(2, 1);

-- SCRIPT CAPTURA USER --
DROP USER IF EXISTS 'infra_watch_captura'@'%';
CREATE USER 'infra_watch_captura'@'%' IDENTIFIED BY 'Urubu100';
GRANT SELECT ON InfraWatch.* TO 'infra_watch_captura'@'%';
FLUSH PRIVILEGES;