SELECT
  SPRIDEN.SPRIDEN_PIDM
  DBMS_RANDOM.STRING ('a',8) as SPRIDEN_FIRST_NAME,
  DBMS_RANDOM.STRING ('a',8) as SPRIDEN_LAST_NAME,
  TO_DATE ('1950-04-01','YYYY-MM-DD') AS SPBPERS_BIRTH_DATE,
  'Bilingual' AS academic_subject_descriptor,
  'Postsecondary' as grade_level_descriptor,
  'Bachelor of Arts' as degree_type_descriptor,
  'Not Selected' AS SPBPERS_SEX
FROM SPRIDEN 
