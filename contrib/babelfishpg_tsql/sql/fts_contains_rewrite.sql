-- This function performs string rewriting for the full text search CONTAINS predicate
-- in Babelfish
-- For example, a T-SQL query 
-- SELECT * FROM t WHERE CONTAINS(txt, '"good old days"')
-- is rewritten into a Postgres query 
-- SELECT * FROM t WHERE to_tsvector('fts_contains', txt) @@ to_tsquery('fts_contains', 'good <-> old <-> days')
-- In particular, the string constant '"good old days"' gets rewritten into 'good <-> old <-> days'
-- This function performs the string rewriting from '"good old days"' to 'good <-> old <-> days'
CREATE OR REPLACE FUNCTION sys.babelfish_fts_contains_rewrite(IN phrase text)
  RETURNS TEXT AS
$$
DECLARE
  joined_text text;
  word text;
BEGIN
  -- generation term not supported
  IF (phrase COLLATE C) SIMILAR TO ('[ ]*FORMSOF[ ]*\(%\)%' COLLATE C) THEN
    RAISE EXCEPTION 'Generation term not supported';
  END IF;

  -- boolean operators not supported
  IF position(('&' COLLATE C) IN (phrase COLLATE "C")) <> 0 OR position(('|' COLLATE C) IN (phrase COLLATE "C")) <> 0 OR position(('&!' COLLATE C) IN (phrase COLLATE "C")) <> 0 THEN
    RAISE EXCEPTION 'Boolean operators not supported';
  END IF;

  IF position((' AND ' COLLATE C) IN UPPER(phrase COLLATE "C")) <> 0 OR position((' OR ' COLLATE C) IN UPPER(phrase COLLATE "C")) <> 0 OR position((' AND NOT ' COLLATE C) IN UPPER(phrase COLLATE "C")) <> 0 THEN
    RAISE EXCEPTION 'Boolean operators not supported';
  END IF;

  -- Initialize the joined_text variable
  joined_text := '';

  -- Strip leading and trailing spaces from the phrase
  phrase := trim(phrase COLLATE "C") COLLATE "C";

  -- no rewriting is needed if the query is a single word
  IF position((' ' COLLATE C) IN (phrase COLLATE "C")) = 0 AND position(('"' COLLATE C) IN UPPER(phrase COLLATE "C")) = 0 THEN
    RETURN phrase;
  END IF;

  -- rewrite phrase queries 
  -- '"word1 word2 word3"' is rewritten into 'word1<->word2<->word3'

  -- Check if the phrase is surrounded by double quotes
  IF position(('"' COLLATE "C") IN (phrase COLLATE "C") ) <> 1 OR position(('"' COLLATE "C") IN (reverse(phrase) COLLATE "C")) <> 1 THEN
    RAISE EXCEPTION 'Phrase must be surrounded by double quotes';
  END IF;

  -- Strip the double quotes from the phrase
  phrase := substring(phrase COLLATE "C", 2, length(phrase) - 2) COLLATE "C";

  -- Strip leading and trailing spaces from the phrase
  phrase := trim(phrase COLLATE "C") COLLATE "C";

  -- Split the phrase into an array of words
  FOREACH word IN ARRAY regexp_split_to_array(phrase COLLATE "C", '\s+' COLLATE "C") COLLATE "C" LOOP
    -- prefix term not supported
    IF (word COLLATE C) SIMILAR TO ('%\*' COLLATE C)  THEN
      RAISE EXCEPTION 'Prefix term not supported';
    END IF;
    -- Append the word to the joined_text variable
    joined_text := joined_text || word || '<->';
  END LOOP;

  -- Remove the trailing "<->" from the joined_text
  joined_text := substring(joined_text COLLATE "C", 1, length(joined_text) - 3) COLLATE "C";

  -- Return the joined_text
  RETURN joined_text;
END;
$$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE; 
-- Removing IMMUTABLE PARALLEL SAFE will disallow parallel mode for full text search