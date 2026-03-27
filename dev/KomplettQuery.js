/**
 * ════════════════════════════════════════════════════════════════════════════
 * LAGERLISTE KOMPLETT QUERY - BIG QUERY TRANSFORMATION (LITE VERSION)
 * ════════════════════════════════════════════════════════════════════════════
 */

const BQ_DATASET = 'imports';
const BQ_TABLE_KOMPLETT = 'lagerliste_komplett';
const LOG_PREFIX = '[KMP-SQL]';
const PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b';

// Validare pe tabelul tău de dimensiuni din Snowflake (aflat în BigQuery)
const SOURCE_TABLE_CANDIDATES = [
  {
    name: 'raw_db_abfrage_t_dim_product',
    requiredCols: ['prod_nr', 'artikeltyp']
  }
];

function _kplLog(phase, message) {
  const timestamp = new Date().toISOString();
  console.log(`${LOG_PREFIX}[${phase}][${timestamp}] ${message}`);
}

function _kplLogBuild(msg) { _kplLog('BUILD', msg); }
function _kplLogExec(seq, msg) { _kplLog(`EXEC-${String(seq).padStart(3, '0')}`, msg); }
function _kplLogDone(msg) { _kplLog('DONE', msg); }
function _kplLogErr(phase, msg) { _kplLog(`ERROR-${phase}`, msg); }

function _kplNormalizeKey(key) {
  if (!key) return '';
  return key
    .toLowerCase()
    .replace(/[äöüß]/g, c => ({ ä: 'ae', ö: 'oe', ü: 'ue', ß: 'ss' }[c]))
    .replace(/[^\w]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

function _kplResolveSourceTable() {
  _kplLogBuild('Resolving source table...');
  let availableTables = [];
  try {
    const tables = BigQuery.Tables.list(PROJECT_ID, BQ_DATASET);
    if (tables.tables) {
      availableTables = tables.tables.map(t => t.tableReference.tableId);
    }
  } catch (e) {
    _kplLogErr('TABLE_LIST', `Could not list tables: ${e.message}`);
  }
  
  for (const candidate of SOURCE_TABLE_CANDIDATES) {
    if (!availableTables.includes(candidate.name)) continue;
    const table = BigQuery.Tables.get(PROJECT_ID, BQ_DATASET, candidate.name);
    if (!table || !table.schema || !table.schema.fields) continue;
    const schemaColumns = table.schema.fields.map(f => _kplNormalizeKey(f.name));
    const missingCols = candidate.requiredCols.filter(col => !schemaColumns.includes(_kplNormalizeKey(col)));
    if (missingCols.length === 0) {
      _kplLogBuild(`✓ SOURCE TABLE RESOLVED: ${candidate.name}`);
      return candidate.name;
    }
  }
  
  for (const tableName of availableTables) {
    if (tableName.startsWith('_')) continue;
    try {
      const table = BigQuery.Tables.get(PROJECT_ID, BQ_DATASET, tableName);
      if (!table || !table.schema || !table.schema.fields) continue;
      const schemaColumns = table.schema.fields.map(f => _kplNormalizeKey(f.name));
      if (schemaColumns.includes('prod_nr') && schemaColumns.includes('artikeltyp')) {
        _kplLogBuild(`✓ AUTO-DETECTED SOURCE TABLE: ${tableName}`);
        return tableName;
      }
    } catch (e) {}
  }
  throw new Error('Source table resolution failed - missing T_DIM_PRODUCT with prod_nr and artikeltyp');
}

function _kplGetTableWithFallback(projectId, tableId) {
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const table = BigQuery.Tables.get(projectId, BQ_DATASET, tableId);
      return { ok: true, table };
    } catch (e) { Utilities.sleep(300); }
  }
  return { ok: false, message: `Table not found: ${tableId}` };
}

function _kplValidateDerivedInputs(sourceTable) {
  const required = [
    { table: sourceTable, columns: ['prod_nr', 'artikeltyp'] },
    // CORECTAT: acum caută cu _pq exact ca în poza ta!
    { table: 'aktionsplan_int_pq', columns: ['laenderspezifische_sap_nummern', 'shop_kopie', 'vk_datum'] }
  ];
  for (const item of required) {
    const tableInfo = _kplGetTableWithFallback(PROJECT_ID, item.table);
    if (!tableInfo.ok) throw new Error(tableInfo.message);
  }
  _kplLogBuild(`Validated derived-input tables successfully.`);
}

function _kplBuildSQL(sourceTable) {
  _kplLogBuild('Building Lagerliste Komplett SQL (Lite Version - Using matched raw_ tables)...');
  
  return `
    WITH aktionsplan_int_base AS (
      SELECT
        SAFE_CAST(laenderspezifische_sap_nummern AS INT64) AS laenderspezifische_sap_nummern,
        CAST(charge AS STRING) AS charge,
        CAST(laendervariante AS STRING) AS laendervariante,
        REPLACE(COALESCE(CAST(shop AS STRING), ''), 'OS', '') AS shop,
        REPLACE(COALESCE(CAST(shop_kopie AS STRING), ''), 'OS', '') AS shop_kopie,
        CAST(liefertermin AS STRING) AS liefertermin,
        SAFE_CAST(vk_datum AS DATE) AS vk_datum,
        CAST(werbeimpuls AS STRING) AS werbeimpuls,
        CAST(wdh AS STRING) AS wdh,
        SAFE_CAST(abverkaufshorizont AS INT64) AS abverkaufshorizont,
        SAFE_CAST(bestellmenge AS INT64) AS bestellmenge,
        SAFE_CAST(aktionsmenge AS INT64) AS aktionsmenge,
        CAST(produktmanager_nat AS STRING) AS produktmanager_nat,
        CAST(saisonkennzeichen AS STRING) AS saisonkennzeichen,
        CAST(thema_nr AS STRING) AS thema_nr,
        CAST(thema AS STRING) AS thema,
        CAST(thema_nat AS STRING) AS thema_nat,
        SAFE_CAST(palettenfaktor AS INT64) AS palettenfaktor,
        SAFE_CAST(lieferdatum AS DATE) AS lieferdatum,
        SAFE_CAST(relevant_zukunftslieferung AS INT64) AS relevant_zukunftslieferung,
        SAFE_CAST(relevantes_vk_datum_ne_bm AS INT64) AS relevantes_vk_datum_ne_bm
      FROM \`${BQ_DATASET}.aktionsplan_int_pq\`
    ),

    source AS (
      SELECT DISTINCT
        a.laenderspezifische_sap_nummern AS sap_kopf,
        a.shop_kopie AS wshop_cd,
        CAST(dim.ARTIKELTYP AS STRING) AS artikeltyp,
        CAST(dim.MARKE AS STRING) AS marke
      FROM aktionsplan_int_base a
      LEFT JOIN \`${BQ_DATASET}.${sourceTable}\` dim
        ON a.laenderspezifische_sap_nummern = SAFE_CAST(dim.PROD_NR AS INT64)
      WHERE a.laenderspezifische_sap_nummern IS NOT NULL
    ),

    kennzahlen_aktionsplan AS (
      SELECT
        laenderspezifische_sap_nummern,
        shop_kopie,
        ARRAY_AGG(thema_nr ORDER BY lieferdatum DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS thema_nr,
        ARRAY_AGG(thema ORDER BY lieferdatum DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS thema_int_bezeichnung,
        ARRAY_AGG(thema_nat ORDER BY lieferdatum DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS thema_nat,
        ARRAY_AGG(palettenfaktor ORDER BY lieferdatum DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS palettenfaktor,
        ARRAY_AGG(laendervariante ORDER BY lieferdatum DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS laendervariante,
        ARRAY_AGG(saisonkennzeichen ORDER BY lieferdatum DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS saisonkennzeichen
      FROM aktionsplan_int_base
      WHERE laenderspezifische_sap_nummern IS NOT NULL
      GROUP BY laenderspezifische_sap_nummern, shop_kopie
    ),

    berechnung_lts_grouped AS (
      SELECT
        laenderspezifische_sap_nummern,
        shop,
        ARRAY_AGG(STRUCT(lieferdatum, bestellmenge) ORDER BY lieferdatum ASC) AS lts
      FROM aktionsplan_int_base
      WHERE laenderspezifische_sap_nummern IS NOT NULL
        AND COALESCE(liefertermin, '') <> ''
        AND lieferdatum IS NOT NULL
        AND relevant_zukunftslieferung = 1
      GROUP BY laenderspezifische_sap_nummern, shop
    ),

    berechnung_lts AS (
      SELECT
        laenderspezifische_sap_nummern,
        shop,
        lts[SAFE_OFFSET(0)].lieferdatum AS lt_1,
        lts[SAFE_OFFSET(1)].lieferdatum AS lt_2,
        lts[SAFE_OFFSET(2)].lieferdatum AS lt_3,
        lts[SAFE_OFFSET(3)].lieferdatum AS lt_4,
        lts[SAFE_OFFSET(0)].bestellmenge AS bestellmenge_1,
        lts[SAFE_OFFSET(1)].bestellmenge AS bestellmenge_2,
        lts[SAFE_OFFSET(2)].bestellmenge AS bestellmenge_3,
        lts[SAFE_OFFSET(3)].bestellmenge AS bestellmenge_4
      FROM berechnung_lts_grouped
    ),

    berechnung_vk_datum_amc_base AS (
      SELECT
        a.*,
        CAST(s.artikeltyp AS STRING) AS artikeltyp
      FROM aktionsplan_int_base a
      LEFT JOIN source s
        ON a.laenderspezifische_sap_nummern = s.sap_kopf
      WHERE a.laenderspezifische_sap_nummern IS NOT NULL
        AND COALESCE(a.liefertermin, '') <> ''
        AND a.lieferdatum IS NOT NULL
        AND a.vk_datum IS NOT NULL
    ),

    berechnung_vk_datum_amc_art AS (
      SELECT
        laenderspezifische_sap_nummern,
        shop,
        MAX(lieferdatum) AS lieferdatum_max_art
      FROM berechnung_vk_datum_amc_base
      GROUP BY laenderspezifische_sap_nummern, shop
    ),

    berechnung_vk_datum_amc_charge AS (
      SELECT
        laenderspezifische_sap_nummern,
        shop,
        charge,
        artikeltyp,
        MIN(vk_datum) AS vk_datum_min,
        MAX(vk_datum) AS vk_datum_max,
        MAX(lieferdatum) AS lieferdatum_max_charge
      FROM berechnung_vk_datum_amc_base
      GROUP BY laenderspezifische_sap_nummern, shop, charge, artikeltyp
    ),

    berechnung_vk_datum_amc_selected AS (
      SELECT
        c.laenderspezifische_sap_nummern,
        c.shop,
        c.charge,
        c.lieferdatum_max_charge,
        CASE
          WHEN c.artikeltyp = 'Festlistung' THEN c.vk_datum_min
          ELSE c.vk_datum_max
        END AS vk_datum_amc
      FROM berechnung_vk_datum_amc_charge c
    ),

    berechnung_vk_datum_amc AS (
      SELECT
        b.laenderspezifische_sap_nummern,
        b.shop,
        s.vk_datum_amc,
        b.werbeimpuls,
        b.abverkaufshorizont AS abverkaufshorizont_amc,
        b.bestellmenge AS bestellmenge_amc
      FROM berechnung_vk_datum_amc_base b
      JOIN berechnung_vk_datum_amc_selected s
        ON b.laenderspezifische_sap_nummern = s.laenderspezifische_sap_nummern
       AND b.shop = s.shop
       AND b.charge = s.charge
       AND b.vk_datum = s.vk_datum_amc
       AND b.lieferdatum = s.lieferdatum_max_charge
      JOIN berechnung_vk_datum_amc_art a
        ON b.laenderspezifische_sap_nummern = a.laenderspezifische_sap_nummern
       AND b.shop = a.shop
       AND a.lieferdatum_max_art = s.lieferdatum_max_charge
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.laenderspezifische_sap_nummern, b.shop
        ORDER BY b.charge
      ) = 1
    ),

    vk_datum_grouped AS (
      SELECT
        laenderspezifische_sap_nummern,
        shop,
        COUNT(1) AS anzahl_impulse,
        ARRAY_AGG(STRUCT(vk_datum, aktionsmenge, werbeimpuls) ORDER BY vk_datum ASC) AS impulse
      FROM aktionsplan_int_base
      WHERE laenderspezifische_sap_nummern IS NOT NULL
        AND vk_datum IS NOT NULL
        AND relevantes_vk_datum_ne_bm = 1
      GROUP BY laenderspezifische_sap_nummern, shop
    ),

    vk_datum AS (
      SELECT
        laenderspezifische_sap_nummern,
        shop,
        anzahl_impulse,
        impulse[SAFE_OFFSET(0)].vk_datum AS vk_datum_1,
        impulse[SAFE_OFFSET(1)].vk_datum AS vk_datum_2,
        impulse[SAFE_OFFSET(2)].vk_datum AS vk_datum_3,
        impulse[SAFE_OFFSET(3)].vk_datum AS vk_datum_4,
        impulse[SAFE_OFFSET(4)].vk_datum AS vk_datum_5,
        impulse[SAFE_OFFSET(5)].vk_datum AS vk_datum_6,
        impulse[SAFE_OFFSET(0)].aktionsmenge AS aktionsmenge_1,
        impulse[SAFE_OFFSET(1)].aktionsmenge AS aktionsmenge_2,
        impulse[SAFE_OFFSET(2)].aktionsmenge AS aktionsmenge_3,
        impulse[SAFE_OFFSET(3)].aktionsmenge AS aktionsmenge_4,
        impulse[SAFE_OFFSET(4)].aktionsmenge AS aktionsmenge_5,
        (
          SELECT SUM(COALESCE(i.aktionsmenge, 0))
          FROM UNNEST(impulse) i WITH OFFSET off
          WHERE off >= 5
        ) AS aktionsmenge_6,
        impulse[SAFE_OFFSET(0)].werbeimpuls AS werbeimpuls_1,
        impulse[SAFE_OFFSET(1)].werbeimpuls AS werbeimpuls_2,
        impulse[SAFE_OFFSET(2)].werbeimpuls AS werbeimpuls_3,
        impulse[SAFE_OFFSET(3)].werbeimpuls AS werbeimpuls_4,
        impulse[SAFE_OFFSET(4)].werbeimpuls AS werbeimpuls_5,
        impulse[SAFE_OFFSET(5)].werbeimpuls AS werbeimpuls_6
      FROM vk_datum_grouped
    ),
    
    -- Join 1: Baugleich Import
    with_baugleich AS (
      SELECT s.*, b.baugleiche_artikel
      FROM source s
      LEFT JOIN \`${BQ_DATASET}.raw_baugleich_import\` b
        ON s.sap_kopf = CAST(b.artikel AS INT64) AND s.wshop_cd = b.land
    ),
    
    -- Join 2: Länderallocation
    with_laenderalloc AS (
      SELECT k.*, la.laenderallokation_andere_laender, la.verfuegbarer_bestand_shop_land,
             la.abgedeckter_restwert_berechnet, la.abgedeckter_restwert_effektiv
      FROM with_baugleich k
      LEFT JOIN (
         SELECT 
           CAST(artikelnummer_1 AS INT64) AS artikelnummer_1, 
           virtuelles_warenhaus,
           CAST(NULL AS FLOAT64) AS laenderallokation_andere_laender,
           CAST(NULL AS FLOAT64) AS verfuegbarer_bestand_shop_land,
           SUM(CAST(abgedeckter_restwert_berechnet AS FLOAT64)) AS abgedeckter_restwert_berechnet,
           SUM(CAST(abgedeckter_restwert_effektiv AS FLOAT64)) AS abgedeckter_restwert_effektiv
         FROM \`${BQ_DATASET}.raw_allocation\`
         GROUP BY 1, 2
      ) la
        ON k.sap_kopf = la.artikelnummer_1 AND k.wshop_cd = la.virtuelles_warenhaus
    ),
    
    -- Join 3: LTs
    with_lts AS (
      SELECT l.*, lt.lt_1, lt.lt_2, lt.lt_3, lt.lt_4, lt.bestellmenge_1, lt.bestellmenge_2, lt.bestellmenge_3, lt.bestellmenge_4
      FROM with_laenderalloc l
      LEFT JOIN berechnung_lts lt
        ON l.sap_kopf = CAST(lt.laenderspezifische_sap_nummern AS INT64) AND l.wshop_cd = lt.shop
    ),
    
    -- Join 4: Menge LTU
    with_ltu AS (
      SELECT l.*, ltu.menge_ltu
      FROM with_lts l
      LEFT JOIN \`${BQ_DATASET}.raw_aktionsplan_int_ltu\` ltu
        ON l.sap_kopf = CAST(ltu.laenderspezifische_sap_nummern AS INT64) AND l.wshop_cd = ltu.shop
    ),
    
    -- Join 5: VK-Datum AMC
    with_vk_amc AS (
      SELECT l.*, vk.vk_datum_amc, vk.werbeimpuls, vk.abverkaufshorizont_amc, vk.bestellmenge_amc
      FROM with_ltu l
      LEFT JOIN berechnung_vk_datum_amc vk
        ON l.sap_kopf = CAST(vk.laenderspezifische_sap_nummern AS INT64) AND l.wshop_cd = vk.shop
    ),
    
    -- Join 6: VK-Datum
    with_vk_datum AS (
      SELECT v.*, vkd.anzahl_impulse, vkd.vk_datum_1, vkd.vk_datum_2, vkd.vk_datum_3, vkd.vk_datum_4, vkd.vk_datum_5, vkd.vk_datum_6,
             vkd.aktionsmenge_1, vkd.aktionsmenge_2, vkd.aktionsmenge_3, vkd.aktionsmenge_4, vkd.aktionsmenge_5, vkd.aktionsmenge_6,
             vkd.werbeimpuls_1, vkd.werbeimpuls_2, vkd.werbeimpuls_3, vkd.werbeimpuls_4, vkd.werbeimpuls_5, vkd.werbeimpuls_6
      FROM with_vk_amc v
      LEFT JOIN vk_datum vkd
        ON v.sap_kopf = CAST(vkd.laenderspezifische_sap_nummern AS INT64) AND v.wshop_cd = vkd.shop
    ),
    
    -- Join 7: Product Ratings Jahre
    with_ratings_jahre AS (
      SELECT p.*, pr.kundenbewertung_letzten_jahre
      FROM with_vk_datum p
      LEFT JOIN \`${BQ_DATASET}.raw_product_ratings_report_jahre\` pr
        ON p.sap_kopf = CAST(pr.head_number AS INT64)
    ),
    
    -- Join 8: Product Ratings 4 Wochen
    with_ratings_4w AS (
      SELECT r.*, pr.kundenbewertung_letzten_4_wochen
      FROM with_ratings_jahre r
      LEFT JOIN \`${BQ_DATASET}.raw_product_ratings_report_4_wochen\` pr
        ON r.sap_kopf = CAST(pr.head_number AS INT64)
    ),
    
    -- Join 9: WT Stationär
    with_wt_stationaer AS (
      SELECT w.*, wt.wt_stationaer_1, wt.wt_stationaer_2, wt.wt_stationaer_3
      FROM with_ratings_4w w
      LEFT JOIN \`${BQ_DATASET}.raw_wt_stationaer\` wt
        ON CAST(w.sap_kopf AS INT64) = CAST(wt.art_nr AS INT64) 
    ),
    
    -- Join 10: Kennzahlen Aktionsplan
    with_aktionsplan AS (
      SELECT p.*, ka.thema_nr, ka.thema_int_bezeichnung, ka.thema_nat, ka.palettenfaktor, ka.laendervariante, ka.saisonkennzeichen
      FROM with_wt_stationaer p
      LEFT JOIN kennzahlen_aktionsplan ka
        ON p.sap_kopf = CAST(ka.laenderspezifische_sap_nummern AS INT64) AND p.wshop_cd = ka.shop_kopie
    ),
    
    -- Join 11: RWA
    with_rwa AS (
      SELECT w.*, rwa.rwa_euro_pro_stueck
      FROM with_aktionsplan w
      LEFT JOIN \`${BQ_DATASET}.rwa_pq\` rwa
        ON w.sap_kopf = CAST(rwa.artikelnummer AS INT64) AND w.wshop_cd = rwa.land
    ),
    
    -- Calculations
    calculated_dates AS (
      SELECT
        * EXCEPT (vk_datum_amc, abverkaufshorizont_amc),
        CASE
          WHEN vk_datum_amc IS NULL OR abverkaufshorizont_amc IS NULL THEN NULL
          ELSE DATE_ADD(vk_datum_amc, INTERVAL CAST(abverkaufshorizont_amc * 7 AS INT64) DAY)
        END AS ende_abverkaufshorizont,
        CASE
          WHEN vk_datum_amc IS NULL OR abverkaufshorizont_amc IS NULL THEN NULL
          WHEN DATE_ADD(vk_datum_amc, INTERVAL CAST(abverkaufshorizont_amc * 7 AS INT64) DAY) < CURRENT_DATE() THEN 'x'
          ELSE NULL
        END AS pruefung_datum_abgelaufen
      FROM with_rwa
    ),
    
    hilfsspalte2 AS (
      SELECT
        * EXCEPT (ende_abverkaufshorizont, pruefung_datum_abgelaufen),
        ende_abverkaufshorizont,
        pruefung_datum_abgelaufen,
        CASE
          WHEN ende_abverkaufshorizont IS NULL OR pruefung_datum_abgelaufen = 'x' THEN NULL
          ELSE DATE_DIFF(ende_abverkaufshorizont, DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)), DAY) / 7.0
        END AS hilfsspalte_2
      FROM calculated_dates
    ),
    
    final_calcs AS (
      SELECT
        *,
        ende_abverkaufshorizont AS neuer_wt_osde,
        CASE
          WHEN ende_abverkaufshorizont < CURRENT_DATE() THEN 'x'
          ELSE ''
        END AS massnahmen_bestandsreduzierung
      FROM hilfsspalte2
    )
    
    SELECT * FROM final_calcs
  `;
}

function runKomplettPowerQuery() {
  _kplLogExec(1, 'Starting Lagerliste Komplett transformation (Lite Version)...');
  try {
    const sourceTable = _kplResolveSourceTable();
    _kplValidateDerivedInputs(sourceTable);
    
    const sql = _kplBuildSQL(sourceTable);
    
    let tableExists = false;
    try {
      const tables = BigQuery.Tables.list(PROJECT_ID, BQ_DATASET);
      tableExists = tables.tables && tables.tables.some(t => t.tableReference.tableId === BQ_TABLE_KOMPLETT);
    } catch (e) {}
    
    if (!tableExists) {
      _kplLogExec(9, `Creating new table...`);
      const createSQL = `CREATE TABLE \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE_KOMPLETT}\` AS ${sql}`;
      const queryJobConfig = { configuration: { query: { query: createSQL, useLegacySql: false } } };
      
      const queryResults = BigQuery.Jobs.insert(queryJobConfig, PROJECT_ID);
      _kplWaitForJobCompletion(PROJECT_ID, queryResults.jobReference.jobId, queryResults.jobReference.location);
      
    } else {
      _kplLogExec(9, `Target table exists, clearing with TRUNCATE...`);
      try { BigQuery.Tables.remove(PROJECT_ID, BQ_DATASET, BQ_TABLE_KOMPLETT); } catch (e) {}
      
      const createSQL = `CREATE OR REPLACE TABLE \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE_KOMPLETT}\` AS ${sql}`;
      const queryJobConfig = { configuration: { query: { query: createSQL, useLegacySql: false } } };
      
      const queryResults = BigQuery.Jobs.insert(queryJobConfig, PROJECT_ID);
      _kplWaitForJobCompletion(PROJECT_ID, queryResults.jobReference.jobId, queryResults.jobReference.location);
    }
    
    _kplLogDone(`Lagerliste Komplett transformation completed successfully!`);
    return { success: true, log: `[KMP-SQL] ✓ Komplett query executed successfully.` };
  } catch (error) {
    _kplLogErr('EXECUTION', error.message);
    return { success: false, log: `[KMP-SQL] ✗ FAILED: ${error.message}` };
  }
}

function _kplWaitForJobCompletion(projectId, jobId, location, maxWaitSeconds = 300) {
  const startTime = Date.now();
  while (Date.now() - startTime < maxWaitSeconds * 1000) {
    const job = BigQuery.Jobs.get(projectId, jobId, { location: location });
    if (job.status && job.status.state === 'DONE') {
      if (job.status.errors) throw new Error(JSON.stringify(job.status.errors));
      return;
    }
    Utilities.sleep(2000);
  }
  throw new Error(`Job timeout`);
}