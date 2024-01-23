\echo Use "ALTER EXTENSION vectors UPDATE TO '0.2.0'" to load this file. \quit

DO LANGUAGE plpgsql $$
    DECLARE
    BEGIN
        IF '@extschema@' != 'vectors' THEN
            RAISE EXCEPTION 'Please read upgrade document in https://docs.pgvecto.rs/admin/upgrading.html#upgrade-from-0-1-x.';
        END IF;
    END;
$$;

CREATE OR REPLACE FUNCTION cast_array_to_vector("array" real[], typmod integer, _explicit boolean) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', 'cast_array_to_vector_wrapper';

CREATE OR REPLACE FUNCTION cast_vector_to_array(vector vector, _typmod integer, _explicit boolean) RETURNS real[]
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', 'cast_vector_to_array_wrapper';
