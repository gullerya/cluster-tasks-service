BEGIN;

ALTER TABLE CLUSTER_TASK_META
    ADD CTSKM_APPLICATION_KEY CHARACTER VARYING(64);

DROP FUNCTION IF EXISTS insert_task;
CREATE FUNCTION insert_task(task_type INTEGER,
                            processor_type CHARACTER VARYING(40),
                            uniqueness_key CHARACTER VARYING(40),
                            concurrency_key CHARACTER VARYING(40),
                            application_key CHARACTER VARYING(64),
                            delay_by_millis BIGINT,
                            body_partition INTEGER,
                            ordering_factor BIGINT,
                            body TEXT) RETURNS BIGINT
AS
$$
DECLARE
    task_id BIGINT;

BEGIN
    INSERT INTO cluster_task_meta (CTSKM_TASK_TYPE, CTSKM_PROCESSOR_TYPE, CTSKM_UNIQUENESS_KEY, CTSKM_CONCURRENCY_KEY,
                                   CTSKM_APPLICATION_KEY, CTSKM_DELAY_BY_MILLIS, CTSKM_BODY_PARTITION,
                                   CTSKM_ORDERING_FACTOR, CTSKM_CREATED, CTSKM_STATUS)
    VALUES (task_type, processor_type, uniqueness_key, concurrency_key,
            application_key, delay_by_millis, body_partition,
            COALESCE(ordering_factor, (EXTRACT(EPOCH FROM LOCALTIMESTAMP) * 10E+8)::BIGINT + delay_by_millis),
            LOCALTIMESTAMP, 0) RETURNING CTSKM_ID INTO task_id;

    IF body_partition = 0 THEN
        INSERT INTO cluster_task_body_p0 (CTSKB_ID, CTSKB_BODY) VALUES (task_id, body);
    ELSEIF body_partition = 1 THEN
        INSERT INTO cluster_task_body_p1 (CTSKB_ID, CTSKB_BODY) VALUES (task_id, body);
    ELSEIF body_partition = 2 THEN
        INSERT INTO cluster_task_body_p2 (CTSKB_ID, CTSKB_BODY) VALUES (task_id, body);
    ELSEIF body_partition = 3 THEN
        INSERT INTO cluster_task_body_p3 (CTSKB_ID, CTSKB_BODY) VALUES (task_id, body);
    END IF;

    RETURN task_id;
END;
$$ LANGUAGE plpgsql;

END;