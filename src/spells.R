library(DBI)


# SQL string to match valid episode data for calculating spells.
valid_episodes_query <- "SELECT * FROM APC WHERE
ADMIDATE_MISSING = FALSE AND
ENCRYPTED_HESID_MISSING = FALSE AND
PROCODE3_MISSING = FALSE AND
EPISTAT = 3 AND
EPIKEY IS NOT NULL AND
EPISTART IS NOT NULL AND
EPIEND IS NOT NULL"


# SQL string to match valid episode data for calculating spells
# where previous admission date matches current admission date
# thus corresponding to NOT a new spell.
not_new_spell_query_1 <- paste("SELECT 
ENCRYPTED_HESID,PROCODE3,ADMIDATE_FILLED,EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY 
FROM 
(SELECT ENCRYPTED_HESID,PROCODE3,EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY,ADMIDATE_FILLED,
LAG (ADMIDATE_FILLED, 1) OVER
(PARTITION BY ENCRYPTED_HESID,PROCODE3 ORDER BY EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY) 
AS PREVIOUS_ADMIDATE FROM 
(", valid_episodes_query, "))
WHERE ADMIDATE_FILLED = PREVIOUS_ADMIDATE")


# SQL string to match valid episode data for calculating spells
# where previous episode start date matches current episode start
# date thus corresponding to NOT a new spell.
not_new_spell_query_2 <- paste("SELECT 
ENCRYPTED_HESID,PROCODE3,EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY 
FROM 
(SELECT ENCRYPTED_HESID,PROCODE3,EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY, 
LAG (EPISTART, 1) OVER
(PARTITION BY ENCRYPTED_HESID,PROCODE3 ORDER BY EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY) 
AS PREVIOUS_EPISTART FROM (", valid_episodes_query, "))
WHERE EPISTART = PREVIOUS_EPISTART")


# SQL string to match valid episode data for calculating spells
# where current episode start date matches previous episode
# end date and was a transfer thus corresponding to NOT a new spell.
not_new_spell_query_3 <- paste("SELECT ENCRYPTED_HESID,DISMETH,EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY 
FROM
(SELECT ENCRYPTED_HESID,DISMETH,EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY FROM 
(SELECT ENCRYPTED_HESID,DISMETH,EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY,
LAG (EPIEND, 1) OVER 
(PARTITION BY ENCRYPTED_HESID,PROCODE3 ORDER BY EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY) 
AS PREVIOUS_EPIEND FROM (", valid_episodes_query, "))
WHERE EPISTART = PREVIOUS_EPIEND)
WHERE DISMETH = 8 OR
DISMETH = 9")


# Part of an SQL query which identifies just matching episodes
match_valid_episodes_query <- function(query) {
paste("WHERE ENCRYPTED_HESID IN 
(SELECT ENCRYPTED_HESID FROM
(", query, "))
AND PROCODE3 IN (SELECT PROCODE3 FROM
(", query, "))
AND EPISTART IN (SELECT EPISTART FROM
(", query, "))
AND EPIEND IN (SELECT EPIEND FROM 
(", query, ")) 
AND EPIORDER IN (SELECT EPIORDER FROM 
(", query, "))
AND TRANSIT IN (SELECT TRANSIT FROM 
(", query, ")) 
AND EPIKEY IN (SELECT EPIKEY FROM 
(", query, "))")
}


# Set new spell to FALSE when search query
# is satisfied.
# Takes an open SQLite database connection.
# Returns nothing, update the database.
set_not_new_spell <- function(db, search_query) {
  dbSendQuery(db, paste("UPDATE APC SET NEW_SPELL = FALSE", match_valid_episodes_query(search_query))) 
}


# Identify and set new spell as default TRUE and SPELL_ID as 1 where episode is valid.
# Takes an open SQLite database connection.
# Returns nothing, update the database.
set_new_spells <- function(db) {
  dbSendQuery(db, paste("UPDATE APC SET NEW_SPELL = TRUE, SPELL_ID = 1", match_valid_episodes_query(valid_episodes_query)))
}


# SQL query to return value of previous episodes spell ID
lag_spell_id_query <- paste("SELECT LAG (SPELL_ID, 1, 1) OVER 
                     (PARTITION BY ENCRYPTED_HESID,PROCODE3,EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY 
                     ORDER BY EPISTART,EPIEND,EPIORDER,TRANSIT,EPIKEY) 
                     AS PREVIOUS_SPELL_ID FROM (", valid_episodes_query, ") WHERE NEW_SPELL = TRUE")


# Updates a spell ID based on the previous spell ID where spell is new.
# Takes an open SQLite database connection.
# Returns nothing, update the database.
update_spell_id <- function(db) {
  dbSendQuery(db, paste("UPDATE APC SET SPELL_ID = (", lag_spell_id_query, ")",
                        match_valid_episodes_query, " AND NEW_SPELL = TRUE"))
}


# Recover APC headers pertinent to the start of an episode
# Takes a table of expected headers
# Returns a string of comma separated headers
first_episode_headers <- function(expected_headers) {
  filter(expected_headers, dataset == "APC" & 
           colnames != "EPIEND" & 
           colnames != "DISDATE" & 
           colnames != "DISDEST" & 
           colnames != "DISMETH" & 
           colnames != "DISREADYDATE" &
           colnames != "EPIKEY") %>% 
    select(colnames)  %>% 
    unlist(use.names = FALSE) %>% 
    paste(collapse = ",")
} 


# Builds SQL query to recover most variables in the first episode in a spell
# from a list of expected headers in the APC dataset.
# Takes a table of expected headers
# Returns an SQL query as a string
first_episode_query <- function(expected_headers) {
  paste("SELECT ", 
        first_episode_headers(expected_headers),
        ",SPELL_ID, EPIKEY AS EPIKEY_ADM, EPI_COUNT
        FROM
        (SELECT *,COUNT() AS EPI_COUNT FROM APC GROUP BY ENCRYPTED_HESID,SPELL_ID)
        WHERE EPIORDER = 1")
}


# SQL query to recover variables concerning the last episode in a spell
last_episode_query <- paste("SELECT ENCRYPTED_HESID,DISDATE,DISDEST,DISMETH,DISREADYDATE,SPELL_ID,
EPIKEY AS EPIKEY_DIS, MAX_EPIEND AS EPIEND, DISDATE_MISSING FROM
(SELECT *,MAX(EPIEND) AS MAX_EPIEND FROM APC GROUP BY ENCRYPTED_HESID,SPELL_ID)")


# Joins data from first and last episode in a spell
# to then create the inpatient spells table (APCS).
# Takes an open SQLite database connection and a 
# table of expected headers
# Returns nothing, creates new table in the database.
create_inpatient_spells_table <- function(db, expected_headers) {
  dbSendQuery(db, paste("CREATE TABLE APCS AS 
           SELECT * FROM 
           (", first_episode_query(expected_headers), ")
           JOIN
           (", last_episode_query, ")
           USING (ENCRYPTED_HESID,SPELL_ID)"))
}


