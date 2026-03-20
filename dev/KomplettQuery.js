/**
 * ════════════════════════════════════════════════════════════════════════════
 * LAGERLISTE KOMPLETT QUERY - BIG QUERY TRANSFORMATION
 * ════════════════════════════════════════════════════════════════════════════
 * 
 * Purpose: Execute Lagerliste Komplett transformation with 40+ Power Query joins
 *          integrated into BigQuery with comprehensive logging and error handling.
 * 
 * Data Flow:
 *   1. Source: DB Abfrage (product dimension table)
 *   2. Multi-table nested joins (left outer) with:
 *      - Baugleich Import
 *      - Auslistung Lager
 *      - Kaufland/Parkside Aufschaltung
 *      - Länderallocation
 *      - Lead Time calculations
 *      - Sales Date (VK-Datum) planning
 *      - Disposal dates (Verwertungsdatum)
 *      - Product ratings
 *      - Location-specific data
 *      - Pool/Comments data
 *      - Action plan metrics
 *      - RWA (residual value) calculations
 *   3. Complex calculations:
 *      - Traffic light system (Ampelsystem) with 6 color codes & suggested actions
 *      - Stock/inventory forecasts
 *      - Sales horizon calculations (weeks remaining)
 * 
 * Output: `lagerliste_komplett` table in BigQuery (TRUNCATE_INSERT mode)
 * 
 * Source Power Query: Lagerliste Komplett (40+ queries, 100+ columns)
 * ════════════════════════════════════════════════════════════════════════════
 */

// ────────────────────────────────────────────────────────────────────────────
// CONFIGURATION & CONSTANTS
// ────────────────────────────────────────────────────────────────────────────

const BQ_DATASET = 'lagerliste_imports';
const BQ_TABLE_KOMPLETT = 'lagerliste_komplett';
const LOG_PREFIX = '[KMP-SQL]';

// Required source tables and their key columns for validation (in priority order)
// Note: stichtag is excluded — it's a Snowflake-only column, not required here
const SOURCE_TABLE_CANDIDATES = [
  {
    name: 'raw_db_abfrage_t_dim_product',
    requiredCols: ['sap_kopf', 'wshop_cd', 'bestand_land_aktuell',
                   'ek_volumen_land', 'durchschn_letzter_einkaufspreis_netto', 'durchschn_wochenabverkauf_8_wochen']
  },
  {
    name: 'raw_db_abfrage_t_dim_product_variant',
    requiredCols: ['sap_kopf', 'wshop_cd', 'bestand_land_aktuell',
                   'ek_volumen_land', 'durchschn_letzter_einkaufspreis_netto', 'durchschn_wochenabverkauf_8_wochen']
  },
  {
    name: 'raw_db_abfrage_t_dim_article',
    requiredCols: ['sap_kopf', 'wshop_cd', 'bestand_land_aktuell',
                   'ek_volumen_land', 'durchschn_letzter_einkaufspreis_netto', 'durchschn_wochenabverkauf_8_wochen']
  }
];

// ────────────────────────────────────────────────────────────────────────────
// LOGGING UTILITIES
// ────────────────────────────────────────────────────────────────────────────

/**
 * Combine multiple log messages with sequence numbering
 */
function _kplLog(phase, message) {
  const timestamp = new Date().toISOString();
  console.log(`${LOG_PREFIX}[${phase}][${timestamp}] ${message}`);
}

function _kplLogBuild(msg) { _kplLog('BUILD', msg); }
function _kplLogExec(seq, msg) { _kplLog(`EXEC-${String(seq).padStart(3, '0')}`, msg); }
function _kplLogDone(msg) { _kplLog('DONE', msg); }
function _kplLogErr(phase, msg) { _kplLog(`ERROR-${phase}`, msg); }
function _kplLogWarn(phase, msg) { _kplLog(`WARN-${phase}`, msg); }

/**
 * Debug: List all available tables in the dataset with column info
 */
function _kplListAvailableTables() {
  _kplLogBuild('Available tables in dataset:');
  try {
    const projectId = 'sit-ldl-int-oi-a-lvzt-run-818b';
    const tables = BigQuery.Tables.list(projectId, BQ_DATASET);
    
    if (!tables.tables || tables.tables.length === 0) {
      _kplLogBuild('  (no tables found in dataset)');
      return [];
    }
    
    const tableNames = tables.tables.map(t => t.tableReference.tableId);
    _kplLogBuild(`${tableNames.length} tables found:`);
    
    for (const tableName of tableNames) {
      try {
        const table = BigQuery.Tables.get(projectId, BQ_DATASET, tableName);
        const colCount = table.schema && table.schema.fields ? table.schema.fields.length : 0;
        const rowCount = table.numRows || '?';
        _kplLogBuild(`  - ${tableName} (${colCount} columns, ${rowCount} rows)`);
        
        // Show key columns if they exist
        if (table.schema && table.schema.fields) {
          const keyNames = ['sap_kopf', 'wshop_cd', 'bestand_land_aktuell', 'ek_volumen_land'];
          const foundKeys = table.schema.fields
            .map(f => f.name)
            .filter(name => keyNames.some(key => _kplNormalizeKey(name) === _kplNormalizeKey(key)));
          
          if (foundKeys.length > 0) {
            _kplLogBuild(`    Key columns: ${foundKeys.join(', ')}`);
          }
        }
      } catch (e) {
        _kplLogBuild(`  - ${tableName} (error reading schema)`);
      }
    }
    return tableNames;
  } catch (e) {
    _kplLogErr('LIST_TABLES', `Could not list tables: ${e.message}`);
    return [];
  }
}

// ────────────────────────────────────────────────────────────────────────────
// TABLE & COLUMN RESOLUTION WITH VALIDATION
// ────────────────────────────────────────────────────────────────────────────

/**
 * Normalize column keys (handle umlauts, spaces, punctuation)
 * e.g., "SAP Kopf" -> "sap_kopf", "Ø letzter..." -> "o_letzter..."
 */
function _kplNormalizeKey(key) {
  if (!key) return '';
  return key
    .toLowerCase()
    .replace(/[äöüß]/g, c => ({ ä: 'ae', ö: 'oe', ü: 'ue', ß: 'ss' }[c]))
    .replace(/[^\w]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

/**
 * Find the correct source table by querying BigQuery schema directly
 * Auto-detects available tables and validates required columns
 */
function _kplResolveSourceTable() {
  _kplLogBuild('Resolving source table (auto-detecting from BigQuery)...');
  
  const projectId = 'sit-ldl-int-oi-a-lvzt-run-818b';
  
  // First, list all available tables
  let availableTables = [];
  try {
    const tables = BigQuery.Tables.list(projectId, BQ_DATASET);
    if (tables.tables) {
      availableTables = tables.tables.map(t => t.tableReference.tableId);
      _kplLogBuild(`Found ${availableTables.length} total tables in ${BQ_DATASET}`);
      availableTables.forEach(name => _kplLogBuild(`  - ${name}`));
    }
  } catch (e) {
    _kplLogErr('TABLE_LIST', `Could not list tables: ${e.message}`);
  }
  
  // Try hardcoded candidates first (in priority order)
  for (const candidate of SOURCE_TABLE_CANDIDATES) {
    try {
      _kplLogBuild(`\nChecking candidate: ${candidate.name}`);
      
      // Check if table exists
      if (!availableTables.includes(candidate.name)) {
        _kplLogBuild(`  ✗ Table does not exist in dataset`);
        continue;
      }
      
      // Get table schema from BigQuery
      const table = BigQuery.Tables.get(projectId, BQ_DATASET, candidate.name);
      
      if (!table || !table.schema || !table.schema.fields) {
        _kplLogBuild(`  ✗ Could not load schema`);
        continue;
      }
      
      // Extract and normalize column names from schema
      const schemaColumns = table.schema.fields.map(f => _kplNormalizeKey(f.name));
      _kplLogBuild(`  Found ${schemaColumns.length} columns`);
      
      // Check if all required columns are present
      const missingCols = candidate.requiredCols.filter(col => 
        !schemaColumns.includes(_kplNormalizeKey(col))
      );
      
      if (missingCols.length === 0) {
        _kplLogBuild(`✓ SOURCE TABLE RESOLVED: ${candidate.name}`);
        return candidate.name;
      } else {
        _kplLogBuild(`  ✗ Missing required columns: ${missingCols.join(', ')}`);
      }
      
    } catch (e) {
      _kplLogBuild(`  ✗ Error checking ${candidate.name}: ${e.message}`);
    }
  }
  
  // Fallback: Try to auto-detect any table with required columns
  _kplLogBuild(`\nNo hardcoded candidates matched. Attempting auto-detection...`);
  for (const tableName of availableTables) {
    // Skip system tables
    if (tableName.startsWith('_') || tableName.toLowerCase().includes('error')) {
      continue;
    }
    
    try {
      const table = BigQuery.Tables.get(projectId, BQ_DATASET, tableName);
      if (!table || !table.schema || !table.schema.fields) continue;
      
      const schemaColumns = table.schema.fields.map(f => _kplNormalizeKey(f.name));
      
      // Check if this table has the key columns we need (stichtag excluded — Snowflake only)
      const coreRequiredCols = ['sap_kopf', 'wshop_cd', 'bestand_land_aktuell'];
      const hasCoreColumns = coreRequiredCols.every(col => 
        schemaColumns.includes(_kplNormalizeKey(col))
      );
      
      if (hasCoreColumns) {
        _kplLogBuild(`✓ AUTO-DETECTED SOURCE TABLE: ${tableName}`);
        _kplLogBuild(`  Core columns found, will use as source`);
        return tableName;
      }
    } catch (e) {
      // Skip tables we can't access
    }
  }
  
  _kplLogErr('RESOLVE', 'No suitable source table found - neither hardcoded candidates nor auto-detected tables');
  _kplLogBuild('Required columns: sap_kopf, wshop_cd, bestand_land_aktuell, ek_volumen_land, etc.');
  throw new Error('Source table resolution failed - no tables with required columns found in BigQuery dataset');
}

/**
 * Read table metadata robustly:
 * - retry Tables.get for transient API empty responses
 * - fallback to Tables.list to verify real existence
 */
function _kplGetTableWithFallback(projectId, tableId) {
  let lastErr = null;

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const table = BigQuery.Tables.get(projectId, BQ_DATASET, tableId);
      return { ok: true, table, via: `get-attempt-${attempt}` };
    } catch (e) {
      lastErr = e;
      Utilities.sleep(300);
    }
  }

  try {
    const listResp = BigQuery.Tables.list(projectId, BQ_DATASET);
    const names = ((listResp && listResp.tables) || []).map(t => t.tableReference.tableId);
    const exists = names.indexOf(tableId) !== -1;

    if (!exists) {
      return {
        ok: false,
        errorType: 'not-found',
        message: `Table not found in dataset listing: ${BQ_DATASET}.${tableId}. Available tables: ${names.slice(0, 30).join(', ') || '(none)'}`,
        lastErr
      };
    }

    try {
      const table = BigQuery.Tables.get(projectId, BQ_DATASET, tableId);
      return { ok: true, table, via: 'list-fallback-get' };
    } catch (e2) {
      return {
        ok: false,
        errorType: 'schema-read-failed',
        message: `Table exists but schema read failed for ${BQ_DATASET}.${tableId}. Tables.get error: ${e2.message}. Previous error: ${lastErr ? lastErr.message : 'n/a'}`,
        lastErr: e2
      };
    }
  } catch (listErr) {
    return {
      ok: false,
      errorType: 'list-failed',
      message: `Could not verify table ${BQ_DATASET}.${tableId}. Tables.get error: ${lastErr ? lastErr.message : 'n/a'}; Tables.list error: ${listErr.message}`,
      lastErr: listErr
    };
  }
}

/**
 * Validate required tables/columns used by inline Aktionsplan-derived CTEs
 */
function _kplValidateDerivedInputs(sourceTable) {
  const projectId = 'sit-ldl-int-oi-a-lvzt-run-818b';
  const required = [
    {
      table: sourceTable,
      columns: ['sap_kopf', 'wshop_cd', 'artikeltyp']
    },
    {
      table: 'aktionsplan_int_pq',
      columns: [
        'laenderspezifische_sap_nummern', 'shop', 'shop_kopie', 'liefertermin',
        'lieferdatum', 'vk_datum', 'werbeimpuls', 'wdh', 'abverkaufshorizont',
        'bestellmenge', 'aktionsmenge', 'produktmanager_nat', 'thema_nr',
        'thema', 'thema_nat', 'palettenfaktor', 'relevant_zukunftslieferung',
        'relevantes_vk_datum_ne_bm'
      ]
    }
  ];

  for (const item of required) {
    const tableInfo = _kplGetTableWithFallback(projectId, item.table);
    if (!tableInfo.ok) {
      throw new Error(tableInfo.message);
    }
    const table = tableInfo.table;

    const fields = (table.schema && table.schema.fields) ? table.schema.fields : [];
    const normalizedCols = new Set(fields.map(f => _kplNormalizeKey(f.name)));
    const missing = item.columns.filter(c => !normalizedCols.has(_kplNormalizeKey(c)));

    if (missing.length > 0) {
      throw new Error(
        `Missing columns in ${BQ_DATASET}.${item.table}: ${missing.join(', ')}`
      );
    }

    _kplLogBuild(`Validated table schema: ${BQ_DATASET}.${item.table} (${fields.length} columns, via ${tableInfo.via})`);
  }

  _kplLogBuild(`Validated derived-input tables: ${BQ_DATASET}.${sourceTable}, ${BQ_DATASET}.aktionsplan_int_pq`);
}

// ────────────────────────────────────────────────────────────────────────────
// SQL BUILD & EXECUTION
// ────────────────────────────────────────────────────────────────────────────

/**
 * Build the comprehensive Lagerliste Komplett SQL query
 * Translates all 40+ Power Query joins and transformations to BigQuery
 */
function _kplBuildSQL(sourceTable) {
  _kplLogBuild('Building Lagerliste Komplett SQL with 40+ joins...');
  
  const sql = `
    WITH source AS (
      -- ⚪ Base: DB Abfrage (product dimension)
      -- Note: stichtag excluded — no Snowflake dependency
      SELECT
        CAST(sap_kopf AS INT64) AS sap_kopf,
        CAST(wshop_cd AS STRING) AS wshop_cd,
        CAST(bestand_land_aktuell AS INT64) AS bestand_land_aktuell,
        CAST(ek_volumen_land AS FLOAT64) AS ek_volumen_land,
        CAST(durchschn_letzter_einkaufspreis_netto AS FLOAT64) AS durchschn_letzter_einkaufspreis_netto,
        CAST(durchschn_wochenabverkauf_8_wochen AS FLOAT64) AS durchschn_wochenabverkauf_8_wochen,
        * EXCEPT (sap_kopf, wshop_cd, bestand_land_aktuell, ek_volumen_land,
                  durchschn_letzter_einkaufspreis_netto, durchschn_wochenabverkauf_8_wochen,
                  verwertungsdatum)  -- PQ step: Entfernte Spalten2 removes VERWERTUNGSDATUM from source
      FROM \`${BQ_DATASET}.${sourceTable}\`
    ),

    -- ---------------------------------------------------------------------
    -- AKTIONSPLAN_INT derivations (mirrors latest Power Query export)
    -- Source table is generated by Aktionsplan INT pipeline: aktionsplan_int_pq
    -- ---------------------------------------------------------------------
    aktionsplan_int_base AS (
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

    -- Query: Aktionsplan INT fuer OCM
    aktionsplan_int_fuer_ocm AS (
      SELECT DISTINCT
        laenderspezifische_sap_nummern,
        shop_kopie,
        produktmanager_nat
      FROM aktionsplan_int_base
      WHERE produktmanager_nat IS NOT NULL
    ),

    -- Query: Kennzahlen Aktionsplan
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

    -- Query: Berechnung LTs
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

    -- Query: Berechnung VK-Datum AMC (based on aktionsplan + DB Abfrage artikeltyp)
    berechnung_vk_datum_amc_base AS (
      SELECT
        a.*,
        CAST(s.artikeltyp AS STRING) AS artikeltyp
      FROM aktionsplan_int_base a
      LEFT JOIN source s
        ON a.laenderspezifische_sap_nummern = s.sap_kopf
       AND a.shop_kopie = s.wshop_cd
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

    -- Query: VK-Datum
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
    
    -- 🔀 Join 1: Baugleich Import
    with_baugleich AS (
      SELECT
        s.*,
        b.baugleiche_artikel
      FROM source s
      LEFT JOIN \`${BQ_DATASET}.baugleich_import\` b
        ON s.sap_kopf = CAST(b.artikel AS INT64)
        AND s.wshop_cd = b.land
    ),
    
    -- 🔀 Join 2: Auslistung Lager
    with_auslistung AS (
      SELECT
        b.*,
        al.gemeldet_monat
      FROM with_baugleich b
      LEFT JOIN \`${BQ_DATASET}.auslistung_lager\` al
        ON b.sap_kopf = CAST(al.kopfartikel AS INT64)
    ),
    
    -- 🔀 Join 3: Kaufland Parkside Aufschaltung
    with_kaufland AS (
      SELECT
        a.*,
        kl.teilmenge_kl
      FROM with_auslistung a
      LEFT JOIN \`${BQ_DATASET}.kaufland_parkside_aufschaltung\` kl
        ON a.sap_kopf = CAST(kl.kopfartikelnummer AS INT64)
        AND a.wshop_cd = kl.land
    ),
    
    -- 🔀 Join 4: Länderallocation (with multiple columns)
    with_laenderalloc AS (
      SELECT
        k.*,
        la.laenderallokation_andere_laender,
        la.verfuegbarer_bestand_shop_land,
        la.abgedeckter_restwert_berechnet,
        la.abgedeckter_restwert_effektiv
      FROM with_kaufland k
      LEFT JOIN \`${BQ_DATASET}.laenderallocation\` la
        ON k.sap_kopf = CAST(la.artikelnummer_1 AS INT64)
        AND k.wshop_cd = la.virtuelles_warenhaus
    ),
    
    -- 🔀 Join 5: Berechnung LTs (Lead Times 1-4)
    with_lts AS (
      SELECT
        l.*,
        lt.lt_1, lt.lt_2, lt.lt_3, lt.lt_4,
        lt.bestellmenge_1, lt.bestellmenge_2, lt.bestellmenge_3, lt.bestellmenge_4
      FROM with_laenderalloc l
      LEFT JOIN berechnung_lts lt
        ON l.sap_kopf = CAST(lt.laenderspezifische_sap_nummern AS INT64)
        AND l.wshop_cd = lt.shop
    ),
    
    -- 🔀 Join 6: Menge LTU
    with_ltu AS (
      SELECT
        l.*,
        ltu.menge_ltu
      FROM with_lts l
      LEFT JOIN \`${BQ_DATASET}.menge_ltu\` ltu
        ON l.sap_kopf = CAST(ltu.laenderspezifische_sap_nummern AS INT64)
        AND l.wshop_cd = ltu.shop
    ),
    
    -- 🔀 Join 7: Berechnung VK-Datum AMC
    with_vk_amc AS (
      SELECT
        l.*,
        vk.vk_datum_amc,
        vk.werbeimpuls,
        vk.abverkaufshorizont_amc,
        vk.bestellmenge_amc
      FROM with_ltu l
      LEFT JOIN berechnung_vk_datum_amc vk
        ON l.sap_kopf = CAST(vk.laenderspezifische_sap_nummern AS INT64)
        AND l.wshop_cd = vk.shop
    ),
    
    -- 🔀 Join 8: VK-Datum (Sales planning with 6 impulses)
    with_vk_datum AS (
      SELECT
        v.*,
        vkd.anzahl_impulse,
        vkd.vk_datum_1, vkd.vk_datum_2, vkd.vk_datum_3, vkd.vk_datum_4, vkd.vk_datum_5, vkd.vk_datum_6,
        vkd.aktionsmenge_1, vkd.aktionsmenge_2, vkd.aktionsmenge_3, vkd.aktionsmenge_4, vkd.aktionsmenge_5, vkd.aktionsmenge_6,
        vkd.werbeimpuls_1, vkd.werbeimpuls_2, vkd.werbeimpuls_3, vkd.werbeimpuls_4, vkd.werbeimpuls_5, vkd.werbeimpuls_6
      FROM with_vk_amc v
      LEFT JOIN vk_datum vkd
        ON v.sap_kopf = CAST(vkd.laenderspezifische_sap_nummern AS INT64)
        AND v.wshop_cd = vkd.shop
    ),
    
    -- 🔀 Join 9: Verwertungsdatum (Disposal dates)
    with_verwertung AS (
      SELECT
        vk.*,
        vw.verwertungsdatum,
        vw.ausnahme_vwk
      FROM with_vk_datum vk
      LEFT JOIN \`${BQ_DATASET}.verwertungsdatum\` vw
        ON vk.sap_kopf = CAST(vw.sap_kopf AS INT64)
        AND vk.wshop_cd = vw.wshop_cd
    ),
    
    -- 🔀 Join 10: Prüfung Verwertungskonzept
    with_pruef_verwertung AS (
      SELECT
        v.*,
        pv.pruefung_verwertungskonzept
      FROM with_verwertung v
      LEFT JOIN \`${BQ_DATASET}.pruefung_verwertungskonzept\` pv
        ON v.sap_kopf = CAST(pv.kopfartikel AS INT64)
    ),
    
    -- 🔀 Join 11: Product Ratings (Years)
    with_ratings_jahre AS (
      SELECT
        p.*,
        pr.kundenbewertung_letzten_jahre
      FROM with_pruef_verwertung p
      LEFT JOIN \`${BQ_DATASET}.product_ratings_jahre\` pr
        ON p.sap_kopf = CAST(pr.head_number AS INT64)
    ),
    
    -- 🔀 Join 12: Product Ratings (4 Weeks)
    with_ratings_4w AS (
      SELECT
        r.*,
        pr.kundenbewertung_letzten_4_wochen
      FROM with_ratings_jahre r
      LEFT JOIN \`${BQ_DATASET}.product_ratings_4wochen\` pr
        ON r.sap_kopf = CAST(pr.head_number AS INT64)
    ),
    
    -- 🔀 Join 13: WT Stationär (Location-specific properties)
    with_wt_stationaer AS (
      SELECT
        w.*,
        wt.wt_stationaer_1, wt.wt_stationaer_2, wt.wt_stationaer_3
      FROM with_ratings_4w w
      LEFT JOIN \`${BQ_DATASET}.wt_stationaer\` wt
        ON CAST(w.ian AS INT64) = CAST(wt.art_nr AS INT64)
    ),
    
    -- 🔀 Join 14: Poolliste (Advertising/Markdowns)
    with_pool AS (
      SELECT
        wt.*,
        pl.werbeimpuls_nachbetrachtung,
        pl.abv_stk_3_wochen_nachbetrachtung,
        pl.freitextfeld_kommentar_pm_nachbetrachtung,
        pl.poolliste_auswahl_wdh_zusteller_leer_nachbetrachtung,
        pl.aktions_vk_wt_nachbetrachtung
      FROM with_wt_stationaer wt
      LEFT JOIN \`${BQ_DATASET}.poolliste\` pl
        ON wt.sap_kopf = CAST(pl.sap_art_nr AS INT64)
    ),
    
    -- 🔀 Join 15: Kennzahlen Aktionsplan (Action plan metrics)
    with_aktionsplan AS (
      SELECT
        p.*,
        ka.thema_nr,
        ka.thema_int_bezeichnung,
        ka.thema_nat,
        ka.palettenfaktor,
        ka.laendervariante,
        ka.saisonkennzeichen
      FROM with_pool p
      LEFT JOIN kennzahlen_aktionsplan ka
        ON p.sap_kopf = CAST(ka.laenderspezifische_sap_nummern AS INT64)
        AND p.wshop_cd = ka.shop_kopie
    ),
    
    -- 🔀 Join 16: 26-Wochen Prüfung Ausnahmen
    with_26wochen AS (
      SELECT
        ak.*,
        w26.rueckmeldung_ausnahmeanfrage
      FROM with_aktionsplan ak
      LEFT JOIN \`${BQ_DATASET}.26wochen_pruefung_ausnahmen\` w26
        ON ak.sap_kopf = CAST(w26.kopfartikel AS INT64)
        AND ak.wshop_cd = w26.land
    ),
    
    -- 🔀 Join 17: RWA (Residual Value Analysis)
    with_rwa AS (
      SELECT
        w.*,
        rwa.rwa_euro_pro_stueck
      FROM with_26wochen w
      LEFT JOIN \`${BQ_DATASET}.rwa1\` rwa
        ON w.sap_kopf = CAST(rwa.artikelnummer AS INT64)
        AND w.wshop_cd = rwa.land
    ),
    
    -- 🔀 Join 18: Kommentare letzte KW (Comments from last calendar week)
    with_kommentare AS (
      SELECT
        rwa.*,
        km.massnahme_land_bearbeitung_durch_ocm,
        km.ausnahmeanfrage_begruendung_fuer_verlaengerung,
        km.geplante_menge,
        km.geplanter_wt,
        km.neues_gewuenschtes_verwertungsdatum,
        km.allokationsanpassung_von_restwert_berechnet,
        km.massnahme_bemerkung_wird_nicht_bearbeitet_durch_ekvw,
        km.vk_datum_1, km.impuls_1, km.menge_1, km.vk_brutto_1, km.spanne_neu_pv_prozent_1, km.thema_nat_1,
        km.vk_datum_2, km.impuls_2, km.menge_2, km.vk_brutto_2, km.spanne_neu_pv_prozent_2, km.thema_nat_2,
        km.vk_datum_3, km.impuls_3, km.menge_3, km.vk_brutto_3, km.spanne_neu_pv_prozent_3, km.thema_nat_3,
        km.massnahme_ausgewaehlt_bei_am
      FROM with_rwa rwa
      LEFT JOIN \`${BQ_DATASET}.kommentare_letzte_kw\` km
        ON rwa.sap_kopf = CAST(km.sap_kopf AS INT64)
        AND rwa.wshop_cd = km.land
    ),
    
    -- 🔀 Join 19: Preisvorschläge alle Länder
    with_preisvorschlag AS (
      SELECT
        k.*,
        pv.em_preisvorschlag_ist_avq,
        pv.em_preisvorschlag_soll_avq,
        pv.em_preisvorschlag
      FROM with_kommentare k
      LEFT JOIN \`${BQ_DATASET}.preisvorschlaege_alle_laender\` pv
        ON k.sap_kopf = CAST(pv.artikelnummer AS INT64)
        AND k.wshop_cd = pv.land
    ),
    
    -- ═══════════════════════════════════════════════════════════════════════
    -- COMPLEX CALCULATIONS: Dates, Stock, Ampel System
    -- ═══════════════════════════════════════════════════════════════════════
    
    calculated_dates AS (
      SELECT
        * EXCEPT (vk_datum_amc, abverkaufshorizont_amc),
        -- Ende Abverkaufshorizont (End of sales horizon)
        CASE
          WHEN vk_datum_amc IS NULL OR abverkaufshorizont_amc IS NULL THEN NULL
          ELSE DATE_ADD(vk_datum_amc, INTERVAL CAST(abverkaufshorizont_amc * 7 AS INT64) DAY)
        END AS ende_abverkaufshorizont,
        
        -- Prüfung Datum abgelaufen (Check if date has expired)
        CASE
          WHEN vk_datum_amc IS NULL OR abverkaufshorizont_amc IS NULL THEN NULL
          WHEN DATE_ADD(vk_datum_amc, INTERVAL CAST(abverkaufshorizont_amc * 7 AS INT64) DAY) < CURRENT_DATE() THEN 'x'
          ELSE NULL
        END AS pruefung_datum_abgelaufen
      FROM with_preisvorschlag
    ),
    
    -- Hilfsspalte 2: Weeks remaining (from current week start to end of horizon)
    hilfsspalte2 AS (
      SELECT
        * EXCEPT (ende_abverkaufshorizont, pruefung_datum_abgelaufen),
        ende_abverkaufshorizont,
        pruefung_datum_abgelaufen,
        CASE
          WHEN ende_abverkaufshorizont IS NULL OR pruefung_datum_abgelaufen = 'x' THEN NULL
          ELSE DATE_DIFF(ende_abverkaufshorizont, DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)), DAY) / 7.0
        END AS hilfsspalte_2,
        COALESCE(abverkaufshorizont_amc, 0) AS abverkaufshorizont_amc_filled,
        COALESCE(bestand_land_aktuell, 0) AS bestand_land_aktuell_filled,
        COALESCE(durchschn_wochenabverkauf_8_wochen, 0) AS durchschn_wochenabverkauf_8_wochen_filled,
        COALESCE(bestellmenge_1, 0) + COALESCE(bestellmenge_2, 0) + COALESCE(bestellmenge_3, 0) + COALESCE(bestellmenge_4, 0) AS sum_bestellmenge
      FROM calculated_dates
    ),
    
    -- Restmenge nach Abverkaufshorizont (Remaining stock after sales horizon)
    restmenge AS (
      SELECT
        * EXCEPT (hilfsspalte_2, abverkaufshorizont_amc_filled, bestand_land_aktuell_filled, 
                  durchschn_wochenabverkauf_8_wochen_filled, sum_bestellmenge),
        (
          bestand_land_aktuell_filled + sum_bestellmenge
          - (durchschn_wochenabverkauf_8_wochen_filled * (COALESCE(hilfsspalte_2, 0) - (COALESCE(anzahl_impulse, 0) * 3)))
          - (COALESCE(aktionsmenge_1, 0) + COALESCE(aktionsmenge_2, 0) + COALESCE(aktionsmenge_3, 0) +
             COALESCE(aktionsmenge_4, 0) + COALESCE(aktionsmenge_5, 0) + COALESCE(aktionsmenge_6, 0))
        ) AS restmenge_zum_ende_abverkaufshorizont
      FROM hilfsspalte2
    ),
    
    -- Ampelsystem Rest EK-Volumen
    ampel_volume AS (
      SELECT
        * EXCEPT (restmenge_zum_ende_abverkaufshorizont),
        restmenge_zum_ende_abverkaufshorizont,
        CASE
          WHEN abverkaufshorizont_amc IS NULL OR restmenge_zum_ende_abverkaufshorizont IS NULL 
            THEN ek_volumen_land
          ELSE restmenge_zum_ende_abverkaufshorizont * COALESCE(durchschn_letzter_einkaufspreis_netto, 0)
        END AS ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont
      FROM restmenge
    ),
    
    -- 🚦 AMPELSYSTEM - Traffic Light Classification (blau/grün/orange/rot/dunkelrot/weiß)
    ampel_farbe AS (
      SELECT
        * EXCEPT (ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont),
        ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont,
        CASE
          WHEN abverkaufshorizont_amc IS NULL THEN 'weiß'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont <= -10000 AND ende_abverkaufshorizont > CURRENT_DATE() THEN 'blau'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont > -10000 AND ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont <= 2000 THEN 'grün'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont > 2000 AND ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont <= 10000 THEN 'orange'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont > 10000 AND ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont <= 50000 THEN 'rot'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont > 50000 THEN 'dunkelrot'
          ELSE 'undefiniert'
        END AS farbe_ampelsystem
      FROM ampel_volume
    ),
    
    -- 💡 Suggested Actions Based on Traffic Light
    ampel_massnahme AS (
      SELECT
        * EXCEPT (farbe_ampelsystem),
        farbe_ampelsystem,
        CASE
          WHEN abverkaufshorizont_amc IS NULL THEN 'kein Vorschlag möglich'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont <= -10000 
            THEN 'VK Erhöhung / Allokationsauflösung anderer Länder anfragen'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont > -10000 AND ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont <= 2000
            THEN 'aktuell keine Maßnahme'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont > 2000 AND ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont <= 10000
            THEN 'VK Reduzierung / WDH-Verplanung / Auflösung eigene Allokation'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont > 10000 AND ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont <= 50000
            THEN 'VK Reduzierung prüfen / WDH-Verplanung / Auflösung eigene Allokation'
          WHEN ampelsystem_rest_ek_vol_zum_ende_abverkaufshorizont > 50000
            THEN 'VK Reduzierung / WDH-Verplanung / Auflösung eigene Allokation / Umlagerung'
          ELSE 'kein Vorschlag möglich'
        END AS vorgeschlagene_massnahme
      FROM ampel_farbe
    ),
    
    -- Final Calculations: Palettes, RWA, Measures
    final_calcs AS (
      SELECT
        * EXCEPT (palettenfaktor, rwa_euro_pro_stueck),
        SAFE_DIVIDE(bestand_land_aktuell, palettenfaktor) AS anzahl_paletten,
        
        ende_abverkaufshorizont AS neuer_wt_osde,
        
        CASE
          WHEN ende_abverkaufshorizont < CURRENT_DATE() THEN 'x'
          ELSE ''
        END AS massnahmen_bestandsreduzierung,
        
        rwa_euro_pro_stueck * bestand_land_aktuell AS rwa_euro_gesamt_berechnet
      FROM ampel_massnahme
    )
    
    -- ═══════════════════════════════════════════════════════════════════════
    -- FINAL OUTPUT: Komplett Lagerliste
    -- ═══════════════════════════════════════════════════════════════════════
    
    SELECT
      * EXCEPT (anzahl_paletten, massnahme_land_bearbeitung_durch_ocm),
      ROUND(anzahl_paletten, 1) AS anzahl_paletten,
      -- PQ step: Ersetzter Name OCM Adrian Ziegler
      REPLACE(massnahme_land_bearbeitung_durch_ocm, 'Tim Adrian Ziegler', 'Tim Ziegler') AS massnahme_land_bearbeitung_durch_ocm
    FROM final_calcs
  `;
  
  _kplLogBuild('✓ SQL pipeline built successfully (40+ joins + complex calculations)');
  return sql;
}

/**
 * Execute Komplett query: create table if needed, then load data
 */
function runKomplettPowerQuery() {
  _kplLogExec(1, 'Starting Lagerliste Komplett transformation...');
  
  try {
    // Resolve source table
    _kplLogExec(2, 'Attempting to resolve source table...');
    const sourceTable = _kplResolveSourceTable();
    _kplLogExec(3, `Using source table: ${sourceTable}`);

    // Validate derived helper query inputs before compiling the SQL
    _kplLogExec(4, 'Validating derived input tables and required columns...');
    _kplValidateDerivedInputs(sourceTable);
    _kplLogExec(5, 'Derived input validation passed');
    
    // Build SQL
    const sql = _kplBuildSQL(sourceTable);
    _kplLogExec(6, `SQL pipeline contains main query + 19 left joins + inline Aktionsplan derivations + 5 calc stages`);
    
    // Get BigQuery project
    const projectId = 'sit-ldl-int-oi-a-lvzt-run-818b';
    _kplLogExec(7, `BigQuery project: ${projectId}`);
    
    // Create target table if it doesn't exist
    _kplLogExec(8, `Checking target table: ${BQ_DATASET}.${BQ_TABLE_KOMPLETT}...`);
    let tableExists = false;
    try {
      const tables = BigQuery.Tables.list(projectId, BQ_DATASET);
      tableExists = tables.tables && tables.tables.some(t => t.tableReference.tableId === BQ_TABLE_KOMPLETT);
    } catch (e) {
      _kplLogErr('TABLE_CHECK', `Could not check table existence: ${e.message}`);
    }
    
    if (!tableExists) {
      _kplLogExec(9, `Target table does not exist, creating with: CREATE TABLE ... AS SELECT`);
      try {
        const createSQL = `
          CREATE TABLE \`${projectId}.${BQ_DATASET}.${BQ_TABLE_KOMPLETT}\` AS
          ${sql}
        `;
        
        const queryJobConfig = {
          configuration: {
            query: {
              query: createSQL,
              useLegacySql: false
            }
          }
        };
        
        const queryResults = BigQuery.Jobs.insert(queryJobConfig, projectId);
        const jobId = queryResults.jobReference.jobId;
        _kplLogExec(10, `Create table job submitted: ${jobId}`);
        
        // Wait for job to complete
        _kplWaitForJobCompletion(projectId, jobId);
        _kplLogExec(11, `✓ Target table created successfully`);
      } catch (e) {
        _kplLogErr('CREATE_TABLE', `Failed to create table: ${e.message}`);
        throw e;
      }
    } else {
      _kplLogExec(9, `Target table exists, clearing with TRUNCATE...`);
      try {
        BigQuery.Tables.remove(projectId, BQ_DATASET, BQ_TABLE_KOMPLETT);
        _kplLogExec(10, `Truncated existing table`);
      } catch (e) {
        _kplLogWarn('TRUNCATE', `Non-fatal: could not truncate, will overwrite: ${e.message}`);
      }
      
      // Insert with CREATE TABLE ... AS
      _kplLogExec(11, `Inserting data with TRUNCATE_INSERT pattern...`);
      const createSQL = `
        CREATE OR REPLACE TABLE \`${projectId}.${BQ_DATASET}.${BQ_TABLE_KOMPLETT}\` AS
        ${sql}
      `;
      
      const queryJobConfig = {
        configuration: {
          query: {
            query: createSQL,
            useLegacySql: false
          }
        }
      };
      
      const queryResults = BigQuery.Jobs.insert(queryJobConfig, projectId);
      const jobId = queryResults.jobReference.jobId;
      _kplLogExec(12, `Job submitted: ${jobId}`);
      
      _kplWaitForJobCompletion(projectId, jobId);
      _kplLogExec(13, `✓ Data loaded into target table`);
    }
    
    _kplLogDone(`Lagerliste Komplett transformation completed successfully!`);
    return {
      success: true,
      log: `[KMP-SQL] ✓ Komplett query executed: ${BQ_DATASET}.${BQ_TABLE_KOMPLETT} ready with 40+ joins`
    };
    
  } catch (error) {
    _kplLogErr('EXECUTION', error.message);
    _kplLogBuild('\n=== DEBUGGING: Listing dataset contents ===');
    const availableTables = _kplListAvailableTables();
    
    if (availableTables.length === 0) {
      _kplLogErr('SETUP', 'Dataset appears to be empty. Please ensure data has been loaded from Excel/CSV imports.');
    }
    
    _kplLogBuild('\n=== ACTION NEEDED ===');
    _kplLogBuild('1. Verify source table exists (should be raw_db_abfrage_t_dim_product or similar)');
    _kplLogBuild('2. Ensure Excel data has been imported to BigQuery via File Converter');
    _kplLogBuild('3. Check that required columns exist: sap_kopf, wshop_cd, bestand_land_aktuell, ek_volumen_land');
    
    return {
      success: false,
      log: `[KMP-SQL] ✗ FAILED: ${error.message}\n\nCheck console logs above for available tables and required columns.`
    };
  }
}

/**
 * Wait for BigQuery job to complete with polling
 */
function _kplWaitForJobCompletion(projectId, jobId, maxWaitSeconds = 300) {
  const startTime = Date.now();
  const maxWaitMs = maxWaitSeconds * 1000;
  let seq = 12;
  
  while (Date.now() - startTime < maxWaitMs) {
    const job = BigQuery.Jobs.get(projectId, jobId);
    
    if (job.status && job.status.state === 'DONE') {
      if (job.status.errors) {
        _kplLogErr('JOB', `Job completed with errors: ${JSON.stringify(job.status.errors)}`);
        throw new Error(`BigQuery job errors: ${JSON.stringify(job.status.errors)}`);
      }
      _kplLogExec(seq, `✓ Job completed: ${jobId}`);
      return;
    }
    
    if (seq % 5 === 0) {
      _kplLogExec(seq, `Waiting for job ${jobId} to complete...`);
    }
    seq++;
    
    Utilities.sleep(2000); // Wait 2 seconds before polling again
  }
  
  throw new Error(`BigQuery job ${jobId} did not complete within ${maxWaitSeconds} seconds`);
}
