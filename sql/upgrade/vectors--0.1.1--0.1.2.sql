\echo Use "ALTER EXTENSION vectors UPDATE TO '0.2.0'" to load this file. \quit

DO LANGUAGE plpgsql $$
    DECLARE
    BEGIN
        IF '@extschema@' != 'vectors' THEN
            RAISE EXCEPTION 'Please read upgrade document in https://docs.pgvecto.rs/admin/upgrading.html#upgrade-from-0-1-x.';
        END IF;
    END;
$$;

DROP FUNCTION ai_embedding_vector;

CREATE OR REPLACE FUNCTION ai_embedding_vector(input text) RETURNS vector
STRICT
LANGUAGE c AS 'MODULE_PATHNAME', 'ai_embedding_vector_wrapper';
