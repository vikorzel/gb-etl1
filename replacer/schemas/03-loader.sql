CREATE TABLE IF NOT EXISTS loader
(
    op_id        BIGSERIAL PRIMARY KEY,
    operation    VARCHAR(128),
    tabname      VARCHAR(128),
    id_least     BIGSERIAL,
    id_greatest  BIGSERIAL
)