// ============================================================================
// CONFIGURATION: AKTIONSPLAN INT PQ QUERY PIPELINE
// ============================================================================
const AIPT_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b';
const AIPT_DATASET_ID = 'lagerliste_imports';
const AIPT_TABLE_ID   = 'aktionsplan_int_pq';

// Source tables created by BigQueryLoader (name = cleanTableName(filename)):
//   "Aktionsplan INT.xlsx"     → raw_aktionsplan_int
//   "BÄF_DE.xlsx"             → raw_baef_de
//   "Gesamt Export CBX.xlsx"  → raw_gesamt_export_cbx
const AIPT_SOURCE_AKTIONSPLAN_TABLE = 'raw_aktionsplan_int';
const AIPT_SOURCE_BAEF_TABLE        = 'raw_baef_de';
const AIPT_SOURCE_CBX_TABLE         = 'raw_gesamt_export_cbx';

const AIPT_AUTO_CREATE_TARGET_TABLE = true; // Create once if missing, never recreate each run
// Write strategy for existing target table:
//   - 'TRUNCATE_INSERT': full refresh (recommended for snapshot-style data)
//   - 'MERGE': sync existing table (update, insert, delete missing)
//   - 'APPEND': insert only
const AIPT_WRITE_MODE = 'TRUNCATE_INSERT';

// ============================================================================
// RUN AKTIONSPLAN INT POWER QUERY LOGIC DIRECTLY IN BIGQUERY
//
// Mirrors the Power Query M-Code for "Aktionsplan INT":
//   1. Load Aktionsplan INT.xlsx → select columns → cast IAN to text
//   2. Append BÄF (raw_baef_de) → filter, rename columns → cast IAN to text
//   3. Left-join Export CBX (raw_gesamt_export_cbx) on IAN
//      → derive Thema Nr. and Thema from CBX (fallback to Aktionsplan values)
//   4. Remove IAN column
//   5. Split Liefertermin by "/" → Liefertermin-KW (INT64), Liefertermin-Jahr (STRING)
//   6. Add calculated date columns:
//        Wochenstart heute, Lieferdatum, Relevant Zukunftslieferung,
//        Relevantes VK-Datum BM, Relevantes VK-Datum <> BM
//   7. Deduplicate on {SAP-Nr, Charge, Shop, Liefertermin, VK-Datum, Werbeimpuls, WDH}
//   8. Add Shop - Kopie (Shop without "OS"), replace "Sonstiges" in Saisonkennzeichen
// ============================================================================
function runAktionsplanINTPowerQuery() {
  try {
    console.log('[AIPT-BQ] Starting Aktionsplan INT Power Query transformation...');

    _ensureAktionsplanTargetTable_();

    // Verify all source tables exist and have required columns
    const srcCheck = _checkAktionsplanSourceTables_();
    if (!srcCheck.ok) {
      return { success: false, log: srcCheck.log };
    }

    const cols = srcCheck.cols; // resolved column names per source table

    const sql = _buildAktionsplanSQL_(cols);

    if (AIPT_WRITE_MODE === 'TRUNCATE_INSERT') {
      console.log('[AIPT-BQ] Write mode TRUNCATE_INSERT: replacing full target contents.');
      _aiptRunBQJob(`TRUNCATE TABLE \`${AIPT_PROJECT_ID}.${AIPT_DATASET_ID}.${AIPT_TABLE_ID}\``);
      _aiptRunBQJob(`
        INSERT INTO \`${AIPT_PROJECT_ID}.${AIPT_DATASET_ID}.${AIPT_TABLE_ID}\`
        ${sql}
      `);
    } else if (AIPT_WRITE_MODE === 'APPEND') {
      console.log('[AIPT-BQ] Write mode APPEND: adding rows only.');
      _aiptRunBQJob(`
        INSERT INTO \`${AIPT_PROJECT_ID}.${AIPT_DATASET_ID}.${AIPT_TABLE_ID}\`
        ${sql}
      `);
    } else {
      console.log('[AIPT-BQ] Write mode MERGE: syncing existing target table (update/insert/delete).');
      _aiptRunBQJob(`
        MERGE \`${AIPT_PROJECT_ID}.${AIPT_DATASET_ID}.${AIPT_TABLE_ID}\` T
        USING (${sql}) S
        ON (T.laenderspezifische_sap_nummern IS NOT DISTINCT FROM S.laenderspezifische_sap_nummern
            AND T.charge IS NOT DISTINCT FROM S.charge
            AND T.shop   IS NOT DISTINCT FROM S.shop
            AND T.liefertermin IS NOT DISTINCT FROM S.liefertermin
            AND T.vk_datum     IS NOT DISTINCT FROM S.vk_datum
            AND T.werbeimpuls  IS NOT DISTINCT FROM S.werbeimpuls
            AND T.wdh          IS NOT DISTINCT FROM S.wdh)
        WHEN MATCHED THEN UPDATE SET
          T.laendervariante             = S.laendervariante,
          T.abverkaufshorizont          = S.abverkaufshorizont,
          T.thema_nat                   = S.thema_nat,
          T.bestellmenge                = S.bestellmenge,
          T.aktionsmenge                = S.aktionsmenge,
          T.produktmanager_nat          = S.produktmanager_nat,
          T.saisonkennzeichen           = S.saisonkennzeichen,
          T.thema_nr                    = S.thema_nr,
          T.thema                       = S.thema,
          T.palettenfaktor              = S.palettenfaktor,
          T.verkaufsfaehig_fuer_de      = S.verkaufsfaehig_fuer_de,
          T.verkaufsfaehig_fuer_be      = S.verkaufsfaehig_fuer_be,
          T.verkaufsfaehig_fuer_nl      = S.verkaufsfaehig_fuer_nl,
          T.verkaufsfaehig_fuer_cz      = S.verkaufsfaehig_fuer_cz,
          T.verkaufsfaehig_fuer_es      = S.verkaufsfaehig_fuer_es,
          T.verkaufsfaehig_fuer_fr      = S.verkaufsfaehig_fuer_fr,
          T.verkaufsfaehig_fuer_pl      = S.verkaufsfaehig_fuer_pl,
          T.verkaufsfaehig_fuer_sk      = S.verkaufsfaehig_fuer_sk,
          T.verkaufsfaehig_fuer_at      = S.verkaufsfaehig_fuer_at,
          T.verkaufsfaehig_fuer_hu      = S.verkaufsfaehig_fuer_hu,
          T.verkaufsfaehig_fuer_dk      = S.verkaufsfaehig_fuer_dk,
          T.verkaufsfaehig_fuer_it      = S.verkaufsfaehig_fuer_it,
          T.liefertermin_kw             = S.liefertermin_kw,
          T.liefertermin_jahr           = S.liefertermin_jahr,
          T.wochenstart_heute           = S.wochenstart_heute,
          T.lieferdatum                 = S.lieferdatum,
          T.relevant_zukunftslieferung  = S.relevant_zukunftslieferung,
          T.relevantes_vk_datum_bm      = S.relevantes_vk_datum_bm,
          T.relevantes_vk_datum_ne_bm   = S.relevantes_vk_datum_ne_bm,
          T.shop_kopie                  = S.shop_kopie,
          T.ocm                         = S.ocm
        WHEN NOT MATCHED THEN
          INSERT ROW
        WHEN NOT MATCHED BY SOURCE THEN
          DELETE
      `);
    }

    const rowCount = _aiptQuerySingleCount_(`
      SELECT COUNT(1) FROM \`${AIPT_PROJECT_ID}.${AIPT_DATASET_ID}.${AIPT_TABLE_ID}\`
    `);

    return {
      success: true,
      rowCount: rowCount,
      log: `[SUCCESS] Aktionsplan INT logic executed in BigQuery. Target rows now: ${rowCount}.`
    };

  } catch (e) {
    console.error(`[AIPT-BQ] Fatal: ${e.message}`);
    console.error(`[AIPT-BQ] Stack trace:\n${e.stack}`);
    return { success: false, log: `[CRITICAL] Aktionsplan INT query failed: ${e.message}` };
  }
}

// ============================================================================
// BUILD THE TRANSFORMATION SQL
// ============================================================================
function _buildAktionsplanSQL_(cols) {
  const A = AIPT_PROJECT_ID + '.' + AIPT_DATASET_ID + '.';

  // Aktionsplan INT source columns (BigQueryLoader-normalised names)
  const a = cols.aktionsplan; // { ian, laenderspezifische_sap_nummern, charge, ... }
  // BÄF source columns
  const b = cols.baef;        // { ian, sap_artikelnummer, vk_datum, werbeimpuls_col, aktionsmenge_col, thema_nat, ocm, filter_col1, filter_col2 }
  // Export CBX source columns
  const c = cols.cbx;         // { ian, thema_am }

  return `
    WITH
    -- -------------------------------------------------------------------------
    -- 1. Export CBX: derive Thema INT Nr / Thema INT Bezeichnung, deduplicate
    -- -------------------------------------------------------------------------
    export_cbx AS (
      SELECT
        CAST(\`${c.ian}\` AS STRING) AS ian,
        TRIM(SPLIT(CAST(\`${c.thema_am}\` AS STRING), ' - ')[SAFE_OFFSET(0)]) AS thema_int_nr,
        TRIM(SPLIT(CAST(\`${c.thema_am}\` AS STRING), ' - ')[SAFE_OFFSET(1)]) AS thema_int_bezeichnung
      FROM \`${A}${AIPT_SOURCE_CBX_TABLE}\`
      WHERE \`${c.ian}\` IS NOT NULL
        AND TRIM(CAST(\`${c.ian}\` AS STRING)) <> ''
      QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(\`${c.ian}\` AS STRING) ORDER BY 1) = 1
    ),

    -- -------------------------------------------------------------------------
    -- 2. BÄF rows: filter, rename, fill missing Aktionsplan columns with NULL
    -- -------------------------------------------------------------------------
    baef AS (
      SELECT
        CAST(\`${b.ian}\` AS STRING)                     AS ian,
        CAST(\`${b.ocm}\` AS STRING)                     AS ocm,
        SAFE_CAST(\`${b.sap_artikelnummer}\` AS INT64)   AS laenderspezifische_sap_nummern,
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(\`${b.vk_datum}\` AS STRING)) AS vk_datum,
        CAST(\`${b.werbeimpuls_col}\` AS STRING)         AS werbeimpuls,
        CAST(\`${b.aktionsmenge_col}\` AS STRING)        AS aktionsmenge,
        CAST(\`${b.thema_nat_col}\` AS STRING)           AS thema_nat,
        -- Columns not provided by BÄF
        CAST(NULL AS STRING) AS charge,
        CAST(NULL AS STRING) AS laendervariante,
        CAST(NULL AS STRING) AS shop,
        CAST(NULL AS STRING) AS liefertermin,
        CAST(NULL AS STRING) AS wdh,
        CAST(NULL AS STRING) AS abverkaufshorizont,
        CAST(NULL AS STRING) AS bestellmenge,
        CAST(NULL AS STRING) AS produktmanager_nat,
        CAST(NULL AS STRING) AS saisonkennzeichen,
        CAST(NULL AS STRING) AS thema_nr_raw,
        CAST(NULL AS STRING) AS thema_raw,
        CAST(NULL AS STRING) AS palettenfaktor,
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
      FROM \`${A}${AIPT_SOURCE_BAEF_TABLE}\`
      WHERE (
        \`${b.filter_meldung}\` IS NULL
        OR LOWER(TRIM(CAST(\`${b.filter_meldung}\` AS STRING))) <> 'x'
      )
      AND (
        \`${b.filter_fehler}\` IS NULL
        OR LOWER(TRIM(CAST(\`${b.filter_fehler}\` AS STRING))) <> 'x'
      )
    ),

    -- -------------------------------------------------------------------------
    -- 3. Aktionsplan INT rows
    -- -------------------------------------------------------------------------
    aktionsplan AS (
      SELECT
        CAST(\`${a.ian}\` AS STRING)                                   AS ian,
        CAST(NULL AS STRING)                                           AS ocm,
        SAFE_CAST(\`${a.laenderspezifische_sap_nummern}\` AS INT64)   AS laenderspezifische_sap_nummern,
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(\`${a.vk_datum}\` AS STRING)) AS vk_datum,
        CAST(\`${a.werbeimpuls}\` AS STRING)                           AS werbeimpuls,
        CAST(\`${a.aktionsmenge}\` AS STRING)                          AS aktionsmenge,
        CAST(\`${a.thema_nat}\` AS STRING)                             AS thema_nat,
        CAST(\`${a.charge}\` AS STRING)                                AS charge,
        CAST(\`${a.laendervariante}\` AS STRING)                       AS laendervariante,
        CAST(\`${a.shop}\` AS STRING)                                  AS shop,
        CAST(\`${a.liefertermin}\` AS STRING)                          AS liefertermin,
        CAST(\`${a.wdh}\` AS STRING)                                   AS wdh,
        CAST(\`${a.abverkaufshorizont}\` AS STRING)                    AS abverkaufshorizont,
        CAST(\`${a.bestellmenge}\` AS STRING)                          AS bestellmenge,
        CAST(\`${a.produktmanager_nat}\` AS STRING)                    AS produktmanager_nat,
        CAST(\`${a.saisonkennzeichen}\` AS STRING)                     AS saisonkennzeichen,
        CAST(\`${a.thema_nr}\` AS STRING)                              AS thema_nr_raw,
        CAST(\`${a.thema}\` AS STRING)                                 AS thema_raw,
        CAST(\`${a.palettenfaktor}\` AS STRING)                        AS palettenfaktor,
        CAST(\`${a.verkaufsfaehig_fuer_de}\` AS STRING)                AS verkaufsfaehig_fuer_de,
        CAST(\`${a.verkaufsfaehig_fuer_be}\` AS STRING)                AS verkaufsfaehig_fuer_be,
        CAST(\`${a.verkaufsfaehig_fuer_nl}\` AS STRING)                AS verkaufsfaehig_fuer_nl,
        CAST(\`${a.verkaufsfaehig_fuer_cz}\` AS STRING)                AS verkaufsfaehig_fuer_cz,
        CAST(\`${a.verkaufsfaehig_fuer_es}\` AS STRING)                AS verkaufsfaehig_fuer_es,
        CAST(\`${a.verkaufsfaehig_fuer_fr}\` AS STRING)                AS verkaufsfaehig_fuer_fr,
        CAST(\`${a.verkaufsfaehig_fuer_pl}\` AS STRING)                AS verkaufsfaehig_fuer_pl,
        CAST(\`${a.verkaufsfaehig_fuer_sk}\` AS STRING)                AS verkaufsfaehig_fuer_sk,
        CAST(\`${a.verkaufsfaehig_fuer_at}\` AS STRING)                AS verkaufsfaehig_fuer_at,
        CAST(\`${a.verkaufsfaehig_fuer_hu}\` AS STRING)                AS verkaufsfaehig_fuer_hu,
        CAST(\`${a.verkaufsfaehig_fuer_dk}\` AS STRING)                AS verkaufsfaehig_fuer_dk,
        CAST(\`${a.verkaufsfaehig_fuer_it}\` AS STRING)                AS verkaufsfaehig_fuer_it
      FROM \`${A}${AIPT_SOURCE_AKTIONSPLAN_TABLE}\`
    ),

    -- -------------------------------------------------------------------------
    -- 4. UNION ALL (Table.Combine equivalent)
    -- -------------------------------------------------------------------------
    combined AS (
      SELECT * FROM aktionsplan
      UNION ALL
      SELECT * FROM baef
    ),

    -- -------------------------------------------------------------------------
    -- 5. Left join Export CBX → derive Thema Nr. and Thema
    -- -------------------------------------------------------------------------
    with_cbx AS (
      SELECT
        c.*,
        COALESCE(NULLIF(TRIM(cbx.thema_int_nr), ''), c.thema_nr_raw)           AS thema_nr,
        COALESCE(NULLIF(TRIM(cbx.thema_int_bezeichnung), ''), c.thema_raw)     AS thema
      FROM combined c
      LEFT JOIN export_cbx cbx
        ON c.ian = cbx.ian
    ),

    -- -------------------------------------------------------------------------
    -- 6. Split Liefertermin → Liefertermin-KW and Liefertermin-Jahr
    --    Format: "KW/Jahr" e.g. "15/2026"  (mirrors PQ Table.SplitColumn by "/")
    -- -------------------------------------------------------------------------
    with_split AS (
      SELECT
        *,
        COALESCE(
          SAFE_CAST(TRIM(SPLIT(liefertermin, '/')[SAFE_OFFSET(0)]) AS INT64),
          0
        )                                                   AS liefertermin_kw,
        TRIM(SPLIT(liefertermin, '/')[SAFE_OFFSET(1)])      AS liefertermin_jahr
      FROM with_cbx
    ),

    -- -------------------------------------------------------------------------
    -- 7. Calculate date columns
    --    Wochenstart heute = Monday of current week (DATE_TRUNC WEEK(MONDAY))
    --    Lieferdatum = Monday of (Jan 1 of Jahr + (KW-1) * 7 days)
    --                  NULL  when KW = 0  (Power Query "11.11.1111" sentinel)
    -- -------------------------------------------------------------------------
    with_dates AS (
      SELECT
        *,
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))        AS wochenstart_heute,
        CASE
          WHEN liefertermin_kw = 0
            OR liefertermin_jahr IS NULL
            OR TRIM(COALESCE(liefertermin_jahr, '')) = ''
          THEN NULL
          ELSE DATE_TRUNC(
            DATE_ADD(
              DATE(SAFE_CAST(TRIM(liefertermin_jahr) AS INT64), 1, 1),
              INTERVAL (liefertermin_kw - 1) * 7 DAY
            ),
            WEEK(MONDAY)
          )
        END                                              AS lieferdatum
      FROM with_split
    ),

    -- -------------------------------------------------------------------------
    -- 8. Relevance flags
    -- -------------------------------------------------------------------------
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
          THEN '1'
          ELSE '0'
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
          THEN '1'
          ELSE '0'
        END AS relevantes_vk_datum_ne_bm
      FROM with_flags
    ),

    -- -------------------------------------------------------------------------
    -- 9. Deduplicate on {SAP-Nr, Charge, Shop, Liefertermin, VK-Datum, Werbeimpuls, WDH}
    --    (mirrors Power Query Table.Distinct)
    -- -------------------------------------------------------------------------
    deduped AS (
      SELECT *
      FROM with_ne_bm
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
          laenderspezifische_sap_nummern,
          charge,
          shop,
          liefertermin,
          vk_datum,
          werbeimpuls,
          wdh
        ORDER BY (SELECT NULL)
      ) = 1
    )

    -- -------------------------------------------------------------------------
    -- 10. Final output
    --     • Drop temporary columns (ian, thema_nr_raw, thema_raw)
    --     • Add Shop-Kopie (Shop with "OS" removed, mirrors Replacer.ReplaceText)
    --     • Replace "Sonstiges" → "" in Saisonkennzeichen
    --     • Replace NULL → 0 in Liefertermin-KW (mirrors Table.ReplaceValue)
    -- -------------------------------------------------------------------------
    SELECT
      laenderspezifische_sap_nummern,
      charge,
      laendervariante,
      shop,
      liefertermin,
      vk_datum,
      werbeimpuls,
      wdh,
      abverkaufshorizont,
      thema_nat,
      bestellmenge,
      aktionsmenge,
      produktmanager_nat,
      CASE WHEN saisonkennzeichen = 'Sonstiges' THEN '' ELSE saisonkennzeichen END AS saisonkennzeichen,
      CAST(thema_nr AS STRING) AS thema_nr,
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
      COALESCE(liefertermin_kw, 0)                   AS liefertermin_kw,
      liefertermin_jahr,
      wochenstart_heute,
      lieferdatum,
      relevant_zukunftslieferung,
      relevantes_vk_datum_bm,
      relevantes_vk_datum_ne_bm,
      REPLACE(COALESCE(shop, ''), 'OS', '')           AS shop_kopie,
      ocm
    FROM deduped
  `;
}

// ============================================================================
// SOURCE TABLE COLUMN DISCOVERY
// ============================================================================
function _checkAktionsplanSourceTables_() {
  const tables = [
    { key: 'aktionsplan', tableId: AIPT_SOURCE_AKTIONSPLAN_TABLE },
    { key: 'baef',        tableId: AIPT_SOURCE_BAEF_TABLE },
    { key: 'cbx',         tableId: AIPT_SOURCE_CBX_TABLE }
  ];

  const result = { aktionsplan: {}, baef: {}, cbx: {} };

  for (let i = 0; i < tables.length; i++) {
    const { key, tableId } = tables[i];
    let fields;
    try {
      const table = BigQuery.Tables.get(AIPT_PROJECT_ID, AIPT_DATASET_ID, tableId);
      fields = ((table.schema && table.schema.fields) || []).map(f => String(f.name || '').toLowerCase());
      console.log(`[AIPT-BQ] Table ${tableId}: ${fields.length} columns found.`);
    } catch (e) {
      return {
        ok: false,
        log: `[ERROR] Source table not found: ${AIPT_DATASET_ID}.${tableId}. Please run BigQueryLoader first. (${e.message})`
      };
    }

    if (key === 'aktionsplan') {
      const ian                     = _pickAiptCol_(fields, ['ian', 'ian_1']);
      const sapNr                   = _pickAiptCol_(fields, ['laenderspezifische_sap_nummern', 'sap_artikelnummer']);
      const charge                  = _pickAiptCol_(fields, ['charge']);
      const laendervariante         = _pickAiptCol_(fields, ['laendervariante']);
      const shop                    = _pickAiptCol_(fields, ['shop']);
      const liefertermin            = _pickAiptCol_(fields, ['liefertermin']);
      const vk_datum                = _pickAiptCol_(fields, ['vk_datum', 'vk___datum']);
      const werbeimpuls             = _pickAiptCol_(fields, ['werbeimpuls', 'werbe_impuls']);
      const wdh                     = _pickAiptCol_(fields, ['wdh']);
      const abverkaufshorizont      = _pickAiptCol_(fields, ['abverkaufshorizont']);
      const thema_nat               = _pickAiptCol_(fields, ['thema_nat']);
      const bestellmenge            = _pickAiptCol_(fields, ['bestellmenge']);
      const aktionsmenge            = _pickAiptCol_(fields, ['aktionsmenge']);
      const produktmanager_nat      = _pickAiptCol_(fields, ['produktmanager_nat']);
      const saisonkennzeichen       = _pickAiptCol_(fields, ['saisonkennzeichen']);
      const thema_nr                = _pickAiptCol_(fields, ['thema_nr', 'thema_nr_']);
      const thema                   = _pickAiptCol_(fields, ['thema']);
      const palettenfaktor          = _pickAiptCol_(fields, ['palettenfaktor']);
      const vdf_de                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_de']);
      const vdf_be                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_be']);
      const vdf_nl                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_nl']);
      const vdf_cz                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_cz']);
      const vdf_es                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_es']);
      const vdf_fr                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_fr']);
      const vdf_pl                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_pl']);
      const vdf_sk                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_sk']);
      const vdf_at                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_at']);
      const vdf_hu                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_hu']);
      const vdf_dk                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_dk']);
      const vdf_it                  = _pickAiptCol_(fields, ['verkaufsfaehig_fuer_it']);

      const missing = [['ian', ian], ['laenderspezifische_sap_nummern', sapNr],
                       ['liefertermin', liefertermin], ['vk_datum', vk_datum]]
        .filter(([name, val]) => !val).map(([name]) => name);
      if (missing.length) {
        return { ok: false, log: `[ERROR] ${tableId}: required columns not found: ${missing.join(', ')}. Available: ${fields.join(', ')}` };
      }

      result.aktionsplan = {
        ian, laenderspezifische_sap_nummern: sapNr, charge: charge || 'charge',
        laendervariante: laendervariante || 'laendervariante', shop: shop || 'shop',
        liefertermin, vk_datum, werbeimpuls: werbeimpuls || 'werbeimpuls', wdh: wdh || 'wdh',
        abverkaufshorizont: abverkaufshorizont || 'abverkaufshorizont', thema_nat: thema_nat || 'thema_nat',
        bestellmenge: bestellmenge || 'bestellmenge', aktionsmenge: aktionsmenge || 'aktionsmenge',
        produktmanager_nat: produktmanager_nat || 'produktmanager_nat',
        saisonkennzeichen: saisonkennzeichen || 'saisonkennzeichen',
        thema_nr: thema_nr || 'thema_nr', thema: thema || 'thema',
        palettenfaktor: palettenfaktor || 'palettenfaktor',
        verkaufsfaehig_fuer_de: vdf_de || 'verkaufsfaehig_fuer_de',
        verkaufsfaehig_fuer_be: vdf_be || 'verkaufsfaehig_fuer_be',
        verkaufsfaehig_fuer_nl: vdf_nl || 'verkaufsfaehig_fuer_nl',
        verkaufsfaehig_fuer_cz: vdf_cz || 'verkaufsfaehig_fuer_cz',
        verkaufsfaehig_fuer_es: vdf_es || 'verkaufsfaehig_fuer_es',
        verkaufsfaehig_fuer_fr: vdf_fr || 'verkaufsfaehig_fuer_fr',
        verkaufsfaehig_fuer_pl: vdf_pl || 'verkaufsfaehig_fuer_pl',
        verkaufsfaehig_fuer_sk: vdf_sk || 'verkaufsfaehig_fuer_sk',
        verkaufsfaehig_fuer_at: vdf_at || 'verkaufsfaehig_fuer_at',
        verkaufsfaehig_fuer_hu: vdf_hu || 'verkaufsfaehig_fuer_hu',
        verkaufsfaehig_fuer_dk: vdf_dk || 'verkaufsfaehig_fuer_dk',
        verkaufsfaehig_fuer_it: vdf_it || 'verkaufsfaehig_fuer_it'
      };
      console.log(`[AIPT-BQ] Aktionsplan columns resolved: ian=${ian}, sap=${sapNr}, liefertermin=${liefertermin}, vk_datum=${vk_datum}`);

    } else if (key === 'baef') {
      const ian             = _pickAiptCol_(fields, ['ian', 'ian_1']);
      const ocm             = _pickAiptCol_(fields, ['ocm']);
      const sap             = _pickAiptCol_(fields, ['sap_artikelnummer', 'laenderspezifische_sap_nummern']);
      const vk_datum        = _pickAiptCol_(fields, ['vk_datum', 'vk___datum']);
      const werbeimpuls_col = _pickAiptCol_(fields, ['werbe_impuls', 'werbeimpuls']);
      const aktionsmenge_col= _pickAiptCol_(fields, ['aktions_menge', 'aktionsmenge']);
      const thema_nat_col   = _pickAiptCol_(fields, ['thema_nat']);
      const filter_meldung  = _pickAiptCol_(fields, ['meldung_fs_zu_spaet', 'meldung_fs_zu_sp_t']);
      const filter_fehler   = _pickAiptCol_(fields, ['fehlermeldung_x_fuer_bediengte_formatierung', 'fehlermeldung_x_f_r_bediengte_formatierung']);

      const missing = [['ian', ian], ['sap_artikelnummer', sap], ['vk_datum', vk_datum]]
        .filter(([, v]) => !v).map(([n]) => n);
      if (missing.length) {
        return { ok: false, log: `[ERROR] ${tableId}: required columns not found: ${missing.join(', ')}. Available: ${fields.join(', ')}` };
      }

      result.baef = {
        ian, ocm: ocm || 'ocm', sap_artikelnummer: sap, vk_datum,
        werbeimpuls_col: werbeimpuls_col || 'werbeimpuls',
        aktionsmenge_col: aktionsmenge_col || 'aktionsmenge',
        thema_nat_col: thema_nat_col || 'thema_nat',
        filter_meldung: filter_meldung || 'meldung_fs_zu_spaet',
        filter_fehler:  filter_fehler  || 'fehlermeldung_x_fuer_bediengte_formatierung'
      };
      console.log(`[AIPT-BQ] BÄF columns resolved: ian=${ian}, sap=${sap}, vk_datum=${vk_datum}, filter1=${filter_meldung}, filter2=${filter_fehler}`);

    } else if (key === 'cbx') {
      const ian     = _pickAiptCol_(fields, ['ian', 'ian_1']);
      const thema_am= _pickAiptCol_(fields, ['thema_am', 'thema_am_1']);

      if (!ian || !thema_am) {
        return { ok: false, log: `[ERROR] ${tableId}: required columns not found (ian=${ian || 'n/a'}, thema_am=${thema_am || 'n/a'}). Available: ${fields.join(', ')}` };
      }
      result.cbx = { ian, thema_am };
      console.log(`[AIPT-BQ] Export CBX columns resolved: ian=${ian}, thema_am=${thema_am}`);
    }
  }

  return { ok: true, cols: result };
}

function _pickAiptCol_(fieldNames, candidates) {
  for (let i = 0; i < candidates.length; i++) {
    if (fieldNames.indexOf(candidates[i]) !== -1) return candidates[i];
  }
  return null;
}

// ============================================================================
// TARGET TABLE: AUTO-CREATE IF MISSING
// ============================================================================
function _ensureAktionsplanTargetTable_() {
  try {
    BigQuery.Tables.get(AIPT_PROJECT_ID, AIPT_DATASET_ID, AIPT_TABLE_ID);
    console.log(`[AIPT-BQ] Target table exists: ${AIPT_PROJECT_ID}.${AIPT_DATASET_ID}.${AIPT_TABLE_ID}`);
    return;
  } catch (e) {
    if (!AIPT_AUTO_CREATE_TARGET_TABLE) {
      throw new Error(
        `Target table does not exist: ${AIPT_PROJECT_ID}.${AIPT_DATASET_ID}.${AIPT_TABLE_ID}. ` +
        `Please create it first or set AIPT_AUTO_CREATE_TARGET_TABLE = true.`
      );
    }
  }

  console.log(`[AIPT-BQ] Target table missing. Auto-creating ${AIPT_PROJECT_ID}.${AIPT_DATASET_ID}.${AIPT_TABLE_ID}...`);
  const tableResource = {
    tableReference: {
      projectId: AIPT_PROJECT_ID,
      datasetId: AIPT_DATASET_ID,
      tableId: AIPT_TABLE_ID
    },
    schema: {
      fields: [
        { name: 'laenderspezifische_sap_nummern', type: 'INT64'  },
        { name: 'charge',                         type: 'STRING' },
        { name: 'laendervariante',                type: 'STRING' },
        { name: 'shop',                           type: 'STRING' },
        { name: 'liefertermin',                   type: 'STRING' },
        { name: 'vk_datum',                       type: 'DATE'   },
        { name: 'werbeimpuls',                    type: 'STRING' },
        { name: 'wdh',                            type: 'STRING' },
        { name: 'abverkaufshorizont',             type: 'STRING' },
        { name: 'thema_nat',                      type: 'STRING' },
        { name: 'bestellmenge',                   type: 'STRING' },
        { name: 'aktionsmenge',                   type: 'STRING' },
        { name: 'produktmanager_nat',             type: 'STRING' },
        { name: 'saisonkennzeichen',              type: 'STRING' },
        { name: 'thema_nr',                       type: 'STRING' },
        { name: 'thema',                          type: 'STRING' },
        { name: 'palettenfaktor',                 type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_de',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_be',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_nl',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_cz',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_es',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_fr',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_pl',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_sk',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_at',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_hu',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_dk',         type: 'STRING' },
        { name: 'verkaufsfaehig_fuer_it',         type: 'STRING' },
        { name: 'liefertermin_kw',                type: 'INT64'  },
        { name: 'liefertermin_jahr',              type: 'STRING' },
        { name: 'wochenstart_heute',              type: 'DATE'   },
        { name: 'lieferdatum',                    type: 'DATE'   },
        { name: 'relevant_zukunftslieferung',     type: 'INT64'  },
        { name: 'relevantes_vk_datum_bm',         type: 'STRING' },
        { name: 'relevantes_vk_datum_ne_bm',      type: 'STRING' },
        { name: 'shop_kopie',                     type: 'STRING' },
        { name: 'ocm',                            type: 'STRING' }
      ]
    }
  };

  BigQuery.Tables.insert(tableResource, AIPT_PROJECT_ID, AIPT_DATASET_ID);
  console.log('[AIPT-BQ] Target table created successfully.');
}

// ============================================================================
// INTERNAL HELPERS
// ============================================================================
function _aiptQuerySingleCount_(sql) {
  const req = { query: sql, useLegacySql: false };
  const res = BigQuery.Jobs.query(req, AIPT_PROJECT_ID);
  const rows = res.rows || [];
  if (!rows.length) return 0;
  const raw = rows[0].f && rows[0].f[0] && rows[0].f[0].v;
  return Number(raw || 0);
}

function _aiptRunBQJob(sql) {
  const preview = sql.replace(/\s+/g, ' ').trim().substring(0, 200);
  console.log(`[JOB] Submitting SQL: ${preview}...`);

  const job = BigQuery.Jobs.insert(
    { configuration: { query: { query: sql, useLegacySql: false } } },
    AIPT_PROJECT_ID
  );
  const jobId    = job.jobReference.jobId;
  const location = job.jobReference.location;
  console.log(`[JOB] Job submitted → jobId: ${jobId} | location: ${location}`);

  for (let i = 0; i < 90; i++) {
    Utilities.sleep(2000);
    let st;
    try {
      st = BigQuery.Jobs.get(AIPT_PROJECT_ID, jobId, { location });
    } catch (pollErr) {
      console.warn(`[JOB]   Poll ${i + 1}/90: API hiccup — ${pollErr.message}. Retrying...`);
      continue;
    }
    const state = st.status.state;
    console.log(`[JOB]   Poll ${i + 1}/90: state=${state}`);
    if (state === 'DONE') {
      if (st.status.errorResult) {
        const { message, reason = 'n/a', location: loc = 'n/a' } = st.status.errorResult;
        console.error(`[JOB] BQ job FAILED — reason: ${reason} | location: ${loc} | message: ${message}`);
        throw new Error(`${message} (reason: ${reason}, location: ${loc})`);
      }
      const stats = st.statistics && st.statistics.query;
      if (stats) {
        console.log(`[JOB] BQ job DONE ✔ — rows affected: ${stats.numDmlAffectedRows || 'n/a'} | bytes processed: ${stats.totalBytesProcessed || 'n/a'}`);
      } else {
        console.log(`[JOB] BQ job DONE ✔`);
      }
      return;
    }
  }
  console.error(`[JOB] TIMEOUT — job "${jobId}" still not DONE after 90 polls (~3 min).`);
  throw new Error(`BigQuery job "${jobId}" did not complete within the timeout window.`);
}
