CREATE TABLE IF NOT EXISTS `boards` (
    `last_modified_threads`     INT8,
    `last_modified_archive`     INT8,
    `id`                        INT2 unsigned       NOT NULL auto_increment UNIQUE  PRIMARY KEY,
    `board`                     TINYTEXT            NOT NULL ,
    `title`                     TEXT,
    `threads`                   MEDIUMTEXT,
    `archive`                   MEDIUMTEXT,
    
    UNIQUE unq_idx_boards_board (`board`(10)),
    INDEX idx_boards_board (`board`(10)),
    INDEX idx_boards_id (`id`)
) ENGINE=%%ENGINE%% CHARSET=%%CHARSET%% COLLATE=%%COLLATE%%;
