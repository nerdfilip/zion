CREATE OR REPLACE PROCEDURE `sit-ldl-int-oi-a-lvzt-run-818b.staging.sp_build_aktionsplan_int_pq`()
BEGIN
  -- 1. Configuration
  DECLARE v_project_id STRING DEFAULT 'sit-ldl-int-oi-a-lvzt-run-818b';
  DECLARE v_source_dataset STRING DEFAULT 'imports';
  DECLARE v_target_dataset STRING DEFAULT 'staging';
  DECLARE v_target_table STRING DEFAULT 'aktionsplan_int_pq';

  -- 2. Variables for dynamic column discovery (Fallback logic)
  DECLARE v_ap_ian STRING;
  DECLARE v_ap_sap STRING;
  DECLARE v_ap_vk_datum STRING;
  DECLARE v_ap_werbeimpuls STRING;
  DECLARE v_ap_thema_nr STRING;
  
  DECLARE v_baef_ian STRING;
  DECLARE v_baef_sap STRING;
  DECLARE v_baef_vk_datum STRING;
  DECLARE v_baef_werbeimpuls STRING;
  DECLARE v_baef_aktionsmenge STRING;
  DECLARE v_baef_filter_meldung STRING;
  DECLARE v_baef_filter_fehler STRING;

  DECLARE v_cbx_ian STRING;
  DECLARE v_cbx_thema_am STRING;

  DECLARE dynamic_sql STRING;

  -- 3. Discover exact column names dynamically (Handling variations)
  
  -- Aktionsplan Table Columns
  SET v_ap_ian = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_aktionsplan_int' AND column_name IN ('ian', 'ian_1') LIMIT 1), 'ian');
  SET v_ap_sap = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_aktionsplan_int' AND column_name IN ('laenderspezifische_sap_nummern', 'sap_artikelnummer') LIMIT 1), 'laenderspezifische_sap_nummern');
  SET v_ap_vk_datum = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_aktionsplan_int' AND column_name IN ('vk_datum', 'vk___datum') LIMIT 1), 'vk_datum');
  SET v_ap_werbeimpuls = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_aktionsplan_int' AND column_name IN ('werbeimpuls', 'werbe_impuls') LIMIT 1), 'werbeimpuls');
  SET v_ap_thema_nr = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_aktionsplan_int' AND column_name IN ('thema_nr', 'thema_nr_') LIMIT 1), 'thema_nr');

  -- BAEF Table Columns
  SET v_baef_ian = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_baef_de' AND column_name IN ('ian', 'ian_1') LIMIT 1), 'ian');
  SET v_baef_sap = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_baef_de' AND column_name IN ('sap_artikelnummer', 'laenderspezifische_sap_nummern') LIMIT 1), 'sap_artikelnummer');
  SET v_baef_vk_datum = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_baef_de' AND column_name IN ('vk_datum', 'vk___datum') LIMIT 1), 'vk_datum');
  SET v_baef_werbeimpuls = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_baef_de' AND column_name IN ('werbe_impuls', 'werbeimpuls') LIMIT 1), 'werbe_impuls');
  SET v_baef_aktionsmenge = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_baef_de' AND column_name IN ('aktions_menge', 'aktionsmenge') LIMIT 1), 'aktions_menge');
  SET v_baef_filter_meldung = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_baef_de' AND column_name IN ('meldung_fs_zu_spaet', 'meldung_fs_zu_sp_t') LIMIT 1), 'meldung_fs_zu_spaet');
  SET v_baef_filter_fehler = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_baef_de' AND column_name IN ('fehlermeldung_x_fuer_bediengte_formatierung', 'fehlermeldung_x_f_r_bediengte_formatierung') LIMIT 1), 'fehlermeldung_x_fuer_bediengte_formatierung');

  -- CBX Table Columns
  SET v_cbx_ian = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_gesamt_export_cbx' AND column_name IN ('ian', 'ian_1') LIMIT 1), 'ian');
  SET v_cbx_thema_am = COALESCE((SELECT column_name FROM `sit-ldl-int-oi-a-lvzt-run-818b.imports.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'raw_gesamt_export_cbx' AND column_name IN ('thema_am', 'thema_am_1') LIMIT 1), 'thema_am');

  -- 4. Construct the Main Query using string concatenation
  SET dynamic_sql = """
    CREATE OR REPLACE TABLE `""" || v_project_id || """.""" || v_target_dataset || """.""" || v_target_table || """` AS 
    WITH 
    -- ==========================================
    -- 1. EXPORT CBX DEDUPLICATION
    -- ==========================================
    export_cbx AS (
      SELECT
        CAST(`""" || v_cbx_ian || """` AS STRING) AS ian,
        TRIM(SPLIT(CAST(`""" || v_cbx_thema_am || """` AS STRING), ' - ')[SAFE_OFFSET(0)]) AS thema_int_nr,
        TRIM(SPLIT(CAST(`""" || v_cbx_thema_am || """` AS STRING), ' - ')[SAFE_OFFSET(1)]) AS thema_int_bezeichnung
      FROM `""" || v_project_id || """.""" || v_source_dataset || """.raw_gesamt_export_cbx`
      WHERE `""" || v_cbx_ian || """` IS NOT NULL
        AND TRIM(CAST(`""" || v_cbx_ian || """` AS STRING)) <> ''
      QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(`""" || v_cbx_ian || """` AS STRING) ORDER BY 1) = 1
    ),

    -- ==========================================
    -- 2. BAEF ROWS (WITH ROBUST SERIAL DATE PARSING)
    -- ==========================================
    baef AS (
      SELECT
        CAST(`""" || v_baef_ian || """` AS STRING) AS ian,
        CAST(`ocm` AS STRING) AS ocm,
        SAFE_CAST(`""" || v_baef_sap || """` AS INT64) AS laenderspezifische_sap_nummern,
        COALESCE(
          SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(`""" || v_baef_vk_datum || """` AS STRING), 1, 10)),
          SAFE.PARSE_DATE('%d.%m.%Y', SUBSTR(CAST(`""" || v_baef_vk_datum || """` AS STRING), 1, 10)),
          SAFE.PARSE_DATE('%d/%m/%Y', SUBSTR(CAST(`""" || v_baef_vk_datum || """` AS STRING), 1, 10)),
          CASE 
            WHEN SAFE_CAST(CAST(`""" || v_baef_vk_datum || """` AS STRING) AS FLOAT64) BETWEEN 44000 AND 49000 
            THEN DATE_ADD(DATE '1899-12-30', INTERVAL CAST(SAFE_CAST(CAST(`""" || v_baef_vk_datum || """` AS STRING) AS FLOAT64) AS INT64) DAY)
            ELSE NULL 
          END
        ) AS vk_datum,
        CAST(`""" || v_baef_werbeimpuls || """` AS STRING) AS werbeimpuls,
        SAFE_CAST(`""" || v_baef_aktionsmenge || """` AS INT64) AS aktionsmenge,
        CAST(`thema_nat` AS STRING) AS thema_nat,
        CAST(NULL AS INT64) AS charge,
        CAST(NULL AS STRING) AS laendervariante,
        CAST(NULL AS STRING) AS shop,
        CAST(NULL AS STRING) AS liefertermin,
        CAST(NULL AS STRING) AS wdh,
        CAST(NULL AS INT64) AS abverkaufshorizont,
        CAST(NULL AS INT64) AS bestellmenge,
        CAST(NULL AS STRING) AS produktmanager_nat,
        CAST(NULL AS STRING) AS saisonkennzeichen,
        CAST(NULL AS STRING) AS thema_nr_raw,
        CAST(NULL AS STRING) AS thema_raw,
        CAST(NULL AS INT64) AS palettenfaktor,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_de,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_be,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_nl,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_cz,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_es,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_fr,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_pl,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_sk,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_at,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_hu,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_dk,
        CAST(NULL AS STRING) AS verkaufsfaehig_fuer_it
      FROM `""" || v_project_id || """.""" || v_source_dataset || """.raw_baef_de`
      WHERE (`""" || v_baef_filter_meldung || """` IS NULL OR LOWER(TRIM(CAST(`""" || v_baef_filter_meldung || """` AS STRING))) <> 'x')
        AND (`""" || v_baef_filter_fehler || """` IS NULL OR LOWER(TRIM(CAST(`""" || v_baef_filter_fehler || """` AS STRING))) <> 'x')
    ),

    -- ==========================================
    -- 3. AKTIONSPLAN ROWS (WITH ROBUST SERIAL DATE PARSING)
    -- ==========================================
    aktionsplan AS (
      SELECT
        CAST(`""" || v_ap_ian || """` AS STRING) AS ian,
        CAST(NULL AS STRING) AS ocm,
        SAFE_CAST(`""" || v_ap_sap || """` AS INT64) AS laenderspezifische_sap_nummern,
        COALESCE(
          SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(`""" || v_ap_vk_datum || """` AS STRING), 1, 10)),
          SAFE.PARSE_DATE('%d.%m.%Y', SUBSTR(CAST(`""" || v_ap_vk_datum || """` AS STRING), 1, 10)),
          SAFE.PARSE_DATE('%d/%m/%Y', SUBSTR(CAST(`""" || v_ap_vk_datum || """` AS STRING), 1, 10)),
          CASE 
            WHEN SAFE_CAST(CAST(`""" || v_ap_vk_datum || """` AS STRING) AS FLOAT64) BETWEEN 44000 AND 49000 
            THEN DATE_ADD(DATE '1899-12-30', INTERVAL CAST(SAFE_CAST(CAST(`""" || v_ap_vk_datum || """` AS STRING) AS FLOAT64) AS INT64) DAY)
            ELSE NULL 
          END
        ) AS vk_datum,
        CAST(`""" || v_ap_werbeimpuls || """` AS STRING) AS werbeimpuls,
        SAFE_CAST(`aktionsmenge` AS INT64) AS aktionsmenge,
        CAST(`thema_nat` AS STRING) AS thema_nat,
        SAFE_CAST(`charge` AS INT64) AS charge,
        CAST(`laendervariante` AS STRING) AS laendervariante,
        CAST(`shop` AS STRING) AS shop,
        CAST(`liefertermin` AS STRING) AS liefertermin,
        CAST(`wdh` AS STRING) AS wdh,
        SAFE_CAST(`abverkaufshorizont` AS INT64) AS abverkaufshorizont,
        SAFE_CAST(`bestellmenge` AS INT64) AS bestellmenge,
        CAST(`produktmanager_nat` AS STRING) AS produktmanager_nat,
        CAST(`saisonkennzeichen` AS STRING) AS saisonkennzeichen,
        CAST(`""" || v_ap_thema_nr || """` AS STRING) AS thema_nr_raw,
        CAST(`thema` AS STRING) AS thema_raw,
        SAFE_CAST(`palettenfaktor` AS INT64) AS palettenfaktor,
        CAST(`verkaufsfaehig_fuer_de` AS STRING) AS verkaufsfaehig_fuer_de,
        CAST(`verkaufsfaehig_fuer_be` AS STRING) AS verkaufsfaehig_fuer_be,
        CAST(`verkaufsfaehig_fuer_nl` AS STRING) AS verkaufsfaehig_fuer_nl,
        CAST(`verkaufsfaehig_fuer_cz` AS STRING) AS verkaufsfaehig_fuer_cz,
        CAST(`verkaufsfaehig_fuer_es` AS STRING) AS verkaufsfaehig_fuer_es,
        CAST(`verkaufsfaehig_fuer_fr` AS STRING) AS verkaufsfaehig_fuer_fr,
        CAST(`verkaufsfaehig_fuer_pl` AS STRING) AS verkaufsfaehig_fuer_pl,
        CAST(`verkaufsfaehig_fuer_sk` AS STRING) AS verkaufsfaehig_fuer_sk,
        CAST(`verkaufsfaehig_fuer_at` AS STRING) AS verkaufsfaehig_fuer_at,
        CAST(`verkaufsfaehig_fuer_hu` AS STRING) AS verkaufsfaehig_fuer_hu,
        CAST(`verkaufsfaehig_fuer_dk` AS STRING) AS verkaufsfaehig_fuer_dk,
        CAST(`verkaufsfaehig_fuer_it` AS STRING) AS verkaufsfaehig_fuer_it
      FROM `""" || v_project_id || """.""" || v_source_dataset || """.raw_aktionsplan_int`
    ),

    -- ==========================================
    -- 4. UNION, JOINS, AND SPLITS
    -- ==========================================
    combined AS (
      SELECT * FROM aktionsplan UNION ALL SELECT * FROM baef
    ),
    with_cbx AS (
      SELECT
        c.*,
        COALESCE(NULLIF(TRIM(cbx.thema_int_nr), ''), c.thema_nr_raw) AS thema_nr,
        COALESCE(NULLIF(TRIM(cbx.thema_int_bezeichnung), ''), c.thema_raw) AS thema
      FROM combined c
      LEFT JOIN export_cbx cbx ON c.ian = cbx.ian
    ),
    with_split AS (
      SELECT
        *,
        -- Split Liefertermin by "/" → KW / Jahr (mirrors Power Query Table.SplitColumn)
        -- Format: "KW/Jahr" e.g. "15/2026"
        COALESCE(
          SAFE_CAST(TRIM(SPLIT(liefertermin, '/')[SAFE_OFFSET(0)]) AS INT64),
          0
        ) AS liefertermin_kw,
        SAFE_CAST(TRIM(SPLIT(liefertermin, '/')[SAFE_OFFSET(1)]) AS INT64) AS liefertermin_jahr
      FROM with_cbx
    ),

    -- ==========================================
    -- 5. DATE CALCULATIONS + 6. RELEVANCE FLAGS (NULL FIXES)
    -- ==========================================
    with_dates AS (
      SELECT
        *,
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AS wochenstart_heute,
        CASE
          WHEN liefertermin_kw = 0
            OR liefertermin_jahr IS NULL
          THEN NULL
          ELSE DATE_TRUNC(
            DATE_ADD(
              DATE(liefertermin_jahr, 1, 1),
              INTERVAL (liefertermin_kw - 1) * 7 DAY
            ),
            WEEK(MONDAY)
          )
        END AS lieferdatum
      FROM with_split
    ),
    with_flags AS (
      SELECT
        *,
        CASE
          WHEN LOWER(TRIM(wdh)) = 'x'  THEN 0
          WHEN lieferdatum IS NULL      THEN 0
          WHEN lieferdatum > DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) THEN 1
          ELSE 0
        END AS relevant_zukunftslieferung,
        CASE
          WHEN (UPPER(TRIM(werbeimpuls)) = 'BM' OR UPPER(TRIM(werbeimpuls)) = 'DBM')
               AND vk_datum >= DATE_TRUNC(CURRENT_DATE(), MONTH)
          THEN 1
          ELSE 0
        END AS relevantes_vk_datum_bm
      FROM with_dates
    ),
    with_ne_bm AS (
      SELECT
        *,
        CASE
          WHEN UPPER(TRIM(werbeimpuls)) = 'BM' OR UPPER(TRIM(werbeimpuls)) = 'DBM'
          THEN relevantes_vk_datum_bm
          WHEN vk_datum >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
          THEN 1
          ELSE 0
        END AS relevantes_vk_datum_ne_bm
      FROM with_flags
    ),

    -- ==========================================
    -- 7. DEDUPLICATION
    -- ==========================================
    deduped AS (
      SELECT *
      FROM with_ne_bm
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY laenderspezifische_sap_nummern, charge, shop, liefertermin, vk_datum, werbeimpuls, wdh
        ORDER BY (SELECT NULL)
      ) = 1
    )

    -- ==========================================
    -- 8. FINAL SELECTION 
    -- ==========================================
    SELECT
      laenderspezifische_sap_nummern,
      charge,
      laendervariante,
      shop,
      liefertermin,
      CAST(vk_datum AS STRING) AS vk_datum,
      werbeimpuls,
      wdh,
      abverkaufshorizont,
      thema_nat,
      bestellmenge,
      aktionsmenge,
      produktmanager_nat,
      CASE WHEN saisonkennzeichen = 'Sonstiges' THEN '' ELSE saisonkennzeichen END AS saisonkennzeichen,
      SAFE_CAST(thema_nr AS FLOAT64) AS thema_nr,
      thema,
      palettenfaktor,
      verkaufsfaehig_fuer_de,
      verkaufsfaehig_fuer_be,
      verkaufsfaehig_fuer_nl,
      verkaufsfaehig_fuer_cz,
      verkaufsfaehig_fuer_es,
      verkaufsfaehig_fuer_fr,
      verkaufsfaehig_fuer_pl,
      verkaufsfaehig_fuer_sk,
      verkaufsfaehig_fuer_at,
      verkaufsfaehig_fuer_hu,
      verkaufsfaehig_fuer_dk,
      verkaufsfaehig_fuer_it,
      COALESCE(liefertermin_kw, 0) AS liefertermin_kw,
      CAST(liefertermin_jahr AS STRING) AS liefertermin_jahr,
      wochenstart_heute,
      lieferdatum,
      relevant_zukunftslieferung,
      relevantes_vk_datum_bm,
      relevantes_vk_datum_ne_bm,
      REPLACE(COALESCE(shop, ''), 'OS', '') AS shop_kopie,
      ocm
    FROM deduped;
  """;

  -- 5. Execute the compiled SQL
  EXECUTE IMMEDIATE dynamic_sql;

END;