// ============================================================================
// CONFIGURATION: RWA QUERY PIPELINE
// ============================================================================
const RWA_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b';
const RWA_DATASET_ID = 'imports';
const RWA_TABLE_ID   = 'rwa_pq';
const RWA_AUTO_CREATE_TARGET_TABLE = true;
const RWA_WRITE_MODE = 'MERGE';

// ============================================================================
// UI TRIGGER
// ============================================================================
function openTransformUI() {
  const html = HtmlService.createHtmlOutputFromFile('TransformationsUI')
    .setWidth(700)
    .setHeight(560)
    .setTitle('SQL Transformations');
  SpreadsheetApp.getUi().showModalDialog(html, 'SQL Transformations');
}

function openTransformationsUI() {
  openTransformUI();
}

// ============================================================================
// RUN RWA POWER QUERY LOGIC DIRECTLY IN BIGQUERY
//
// Source tables: raw_osfl_..._rwa
// Target table:  rwa
// ============================================================================
function runExportRWAPowerQuery() {
  try {
    console.log('[RWA-BQ] Starting RWA Power Query transformation...');

    _ensureRWATargetTable_();
    _ensureRWATargetSchema_();

    const sourceTables = _getRWASourceTables_();
    if (sourceTables.length === 0) {
      return {
        success: false,
        log: '[ERROR] No RWA source tables found. Expected names like raw_osfl_..._rwa.'
      };
    }

    const sourceSpecs = _buildRWASourceSpecs_(sourceTables);
    if (!sourceSpecs.length) {
      return {
        success: false,
        log: '[ERROR] No usable RWA source tables found. Required source columns similar to Column1/Column2 or Artikelnummer/RWA.'
      };
    }

    console.log(`[RWA-BQ] Usable source tables (${sourceSpecs.length}): ${sourceSpecs.map(s => s.tableName).join(', ')}`);

    const unionSql = sourceSpecs.map(spec => {
      const land = _extractRWALandFromTableName_(spec.tableName);
      return `
        SELECT
          '${land}' AS land,
          CAST(\`${spec.articleCol}\` AS STRING) AS artikel_raw,
          CAST(\`${spec.rwaCol}\` AS STRING) AS rwa_raw
        FROM \`${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${spec.tableName}\`
      `.trim();
    }).join('\nUNION ALL\n');

    const finalSql = `
      WITH unified AS (
        ${unionSql}
      ),
      typed AS (
        SELECT
          land,
          SAFE_CAST(NULLIF(TRIM(artikel_raw), '') AS INT64) AS artikelnummer,
          SAFE_CAST(
            CASE
              WHEN rwa_raw IS NULL OR TRIM(rwa_raw) = '' THEN NULL
              ELSE (
                -- Build a canonical decimal string with dot as decimal separator and no thousands separators.
                SELECT CASE
                  WHEN cleaned = '' THEN NULL
                  WHEN has_comma AND has_dot THEN
                    CASE
                      WHEN last_comma_pos > last_dot_pos
                        THEN REPLACE(REPLACE(cleaned, '.', ''), ',', '.')
                      ELSE REPLACE(cleaned, ',', '')
                    END
                  WHEN has_comma THEN REPLACE(cleaned, ',', '.')
                  ELSE cleaned
                END
                FROM (
                  SELECT
                    REGEXP_REPLACE(TRIM(rwa_raw), r'[^0-9,.-]', '') AS cleaned,
                    REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(rwa_raw), r'[^0-9,.-]', ''), r',') AS has_comma,
                    REGEXP_CONTAINS(REGEXP_REPLACE(TRIM(rwa_raw), r'[^0-9,.-]', ''), r'\.') AS has_dot,
                    STRPOS(REVERSE(REGEXP_REPLACE(TRIM(rwa_raw), r'[^0-9,.-]', '')), ',') AS rev_comma_pos,
                    STRPOS(REVERSE(REGEXP_REPLACE(TRIM(rwa_raw), r'[^0-9,.-]', '')), '.') AS rev_dot_pos,
                    LENGTH(REGEXP_REPLACE(TRIM(rwa_raw), r'[^0-9,.-]', '')) AS len_clean
                ) p,
                UNNEST([
                  STRUCT(
                    CASE WHEN p.rev_comma_pos = 0 THEN 0 ELSE p.len_clean - p.rev_comma_pos + 1 END AS last_comma_pos,
                    CASE WHEN p.rev_dot_pos = 0 THEN 0 ELSE p.len_clean - p.rev_dot_pos + 1 END AS last_dot_pos
                  )
                ])
              )
            END
            AS BIGNUMERIC
          ) AS rwa_pro_stueck,
          artikel_raw,
          rwa_raw
        FROM unified
      ),
      filtered AS (
        SELECT
          land,
          artikelnummer,
          rwa_pro_stueck
        FROM typed
        WHERE artikel_raw IS NOT NULL
          AND TRIM(artikel_raw) NOT IN ('', 'Gesamtergebnis', 'KopfArtikel', 'Summe von ST RWA')
          AND artikelnummer IS NOT NULL
          AND rwa_pro_stueck IS NOT NULL
          AND rwa_pro_stueck != 0
      )
      SELECT land, artikelnummer, rwa_pro_stueck
      FROM filtered
    `;

    _logRWADiagnostics_(finalSql);

    if (RWA_WRITE_MODE === 'APPEND') {
      console.log('[RWA-BQ] Write mode APPEND: adding rows to target.');
      _rwaRunBQJob(`
        INSERT INTO \`${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}\` (land, artikelnummer, rwa_pro_stueck)
        ${finalSql}
      `);
    } else if (RWA_WRITE_MODE === 'TRUNCATE_INSERT') {
      console.log('[RWA-BQ] Write mode TRUNCATE_INSERT: refreshing target contents.');
      _rwaRunBQJob(`TRUNCATE TABLE \`${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}\``);
      _rwaRunBQJob(`
        INSERT INTO \`${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}\` (land, artikelnummer, rwa_pro_stueck)
        ${finalSql}
      `);
    } else {
      console.log('[RWA-BQ] Write mode MERGE: syncing existing target table (update/insert/delete).');
      _rwaRunBQJob(`
        MERGE \`${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}\` T
        USING (
          ${finalSql}
        ) S
        ON T.land = S.land
           AND T.artikelnummer = S.artikelnummer
        WHEN MATCHED THEN
          UPDATE SET T.rwa_pro_stueck = S.rwa_pro_stueck
        WHEN NOT MATCHED THEN
          INSERT (land, artikelnummer, rwa_pro_stueck)
          VALUES (S.land, S.artikelnummer, S.rwa_pro_stueck)
        WHEN NOT MATCHED BY SOURCE THEN
          DELETE
      `);
    }

    const rowCount = _rwaQuerySingleCount_(`
      SELECT COUNT(1)
      FROM \`${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}\`
    `);

    return {
      success: true,
      rowCount: rowCount,
      log: `[SUCCESS] RWA logic executed in BigQuery. Source tables: ${sourceSpecs.length}. Target rows now: ${rowCount}.`
    };
  } catch (e) {
    console.error(`[RWA-BQ] Fatal: ${e.message}`);
    console.error(`[RWA-BQ] Stack trace:\n${e.stack}`);
    return { success: false, log: `[CRITICAL] RWA query failed: ${e.message}` };
  }
}

function _extractRWALandFromTableName_(tableName) {
  const firstFour = tableName.replace(/^raw_/, '').substring(0, 4).toUpperCase();
  return firstFour.replace(/^OS/, '');
}

function _getRWASourceTables_() {
  const pattern = /^raw_os[a-z]{2}.*_rwa$/;
  let pageToken;
  const matches = [];

  do {
    const list = BigQuery.Tables.list(RWA_PROJECT_ID, RWA_DATASET_ID, { maxResults: 1000, pageToken: pageToken });
    const tables = list.tables || [];
    tables.forEach(t => {
      const tableId = t.tableReference && t.tableReference.tableId;
      if (tableId && pattern.test(tableId)) {
        matches.push(tableId);
      }
    });
    pageToken = list.nextPageToken;
  } while (pageToken);

  return matches.sort();
}

function _buildRWASourceSpecs_(sourceTables) {
  const specs = [];

  sourceTables.forEach(tableName => {
    try {
      const table = BigQuery.Tables.get(RWA_PROJECT_ID, RWA_DATASET_ID, tableName);
      const schemaFields = (table.schema && table.schema.fields) || [];
      const orderedFields = schemaFields.map(f => String(f.name || '').toLowerCase());
      const articleCol = _pickRWAColumn_(orderedFields, ['column1', 'col_1', 'artikelnummer', 'article_number', 'article_number_1']) || orderedFields[0] || null;
      const rwaCol = _pickRWAColumn_(orderedFields, ['column2', 'col_2', 'rwa_pro_st_ck', 'rwa_pro_stueck', 'summe_von_st_rwa', 'rwa']) || orderedFields[1] || null;

      if (!articleCol || !rwaCol) {
        console.log(`[RWA-BQ] Skipping table ${tableName}: missing required columns (article=${articleCol || 'n/a'}, rwa=${rwaCol || 'n/a'}).`);
        return;
      }

      specs.push({ tableName: tableName, articleCol: articleCol, rwaCol: rwaCol });
      console.log(`[RWA-BQ] Table ${tableName}: article=${articleCol}, rwa=${rwaCol}, schema=${orderedFields.join(', ')}`);
    } catch (e) {
      console.log(`[RWA-BQ] Skipping table ${tableName}: schema read failed (${e.message}).`);
    }
  });

  return specs;
}

function _pickRWAColumn_(fieldNames, candidates) {
  for (let i = 0; i < candidates.length; i++) {
    if (fieldNames.indexOf(candidates[i]) !== -1) return candidates[i];
  }
  return null;
}

function _logRWADiagnostics_(finalSql) {
  const sql = `
    WITH final_rows AS (
      ${finalSql}
    )
    SELECT land, COUNT(1) AS row_count
    FROM final_rows
    GROUP BY land
    ORDER BY land
  `;

  const res = BigQuery.Jobs.query({ query: sql, useLegacySql: false }, RWA_PROJECT_ID);
  const rows = res.rows || [];
  if (!rows.length) {
    console.log('[RWA-BQ] Diagnostics: no rows returned.');
    return;
  }

  console.log('[RWA-BQ] Diagnostics by land:');
  rows.forEach(r => {
    const f = r.f || [];
    console.log(`[RWA-BQ]   ${f[0] && f[0].v}: rows=${f[1] && f[1].v}`);
  });
}

function _ensureRWATargetTable_() {
  try {
    BigQuery.Tables.get(RWA_PROJECT_ID, RWA_DATASET_ID, RWA_TABLE_ID);
    console.log(`[RWA-BQ] Target table exists: ${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}`);
    return;
  } catch (e) {
    if (!RWA_AUTO_CREATE_TARGET_TABLE) {
      throw new Error(
        `Target table does not exist: ${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}. ` +
        `Please create it first with columns: land STRING, artikelnummer INT64, rwa_pro_stueck BIGNUMERIC.`
      );
    }
  }

  console.log(`[RWA-BQ] Target table missing. Auto-creating ${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}...`);
  BigQuery.Tables.insert({
    tableReference: { projectId: RWA_PROJECT_ID, datasetId: RWA_DATASET_ID, tableId: RWA_TABLE_ID },
    schema: {
      fields: [
        { name: 'land', type: 'STRING' },
        { name: 'artikelnummer', type: 'INT64' },
        { name: 'rwa_pro_stueck', type: 'BIGNUMERIC' }
      ]
    }
  }, RWA_PROJECT_ID, RWA_DATASET_ID);
  console.log('[RWA-BQ] Target table created successfully.');
}

function _ensureRWATargetSchema_() {
  const table = BigQuery.Tables.get(RWA_PROJECT_ID, RWA_DATASET_ID, RWA_TABLE_ID);
  const fields = (table.schema && table.schema.fields) || [];
  const rwaField = fields.find(f => String(f.name || '').toLowerCase() === 'rwa_pro_stueck');
  if (!rwaField) {
    throw new Error(`Target table ${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID} is missing column rwa_pro_stueck.`);
  }

  if (String(rwaField.type || '').toUpperCase() !== 'BIGNUMERIC') {
    console.log(`[RWA-BQ] Upgrading column rwa_pro_stueck from ${rwaField.type} to BIGNUMERIC...`);
    _rwaRunBQJob(`
      ALTER TABLE \`${RWA_PROJECT_ID}.${RWA_DATASET_ID}.${RWA_TABLE_ID}\`
      ALTER COLUMN rwa_pro_stueck SET DATA TYPE BIGNUMERIC
    `);
    console.log('[RWA-BQ] Column rwa_pro_stueck is now BIGNUMERIC.');
  }
}

function _rwaQuerySingleCount_(sql) {
  const res = BigQuery.Jobs.query({ query: sql, useLegacySql: false }, RWA_PROJECT_ID);
  const rows = res.rows || [];
  if (!rows.length) return 0;
  return Number(rows[0].f && rows[0].f[0] && rows[0].f[0].v || 0);
}

function _rwaRunBQJob(sql) {
  const preview = sql.replace(/\s+/g, ' ').trim().substring(0, 200);
  console.log(`[RWA-JOB] Submitting SQL: ${preview}...`);

  const job = BigQuery.Jobs.insert(
    { configuration: { query: { query: sql, useLegacySql: false } } },
    RWA_PROJECT_ID
  );
  const jobId = job.jobReference.jobId;
  const location = job.jobReference.location;
  console.log(`[RWA-JOB] Job submitted → jobId: ${jobId} | location: ${location}`);

  for (let i = 0; i < 90; i++) {
    Utilities.sleep(2000);
    let st;
    try {
      st = BigQuery.Jobs.get(RWA_PROJECT_ID, jobId, { location: location });
    } catch (pollErr) {
      console.warn(`[RWA-JOB]   Poll ${i + 1}/90: API hiccup — ${pollErr.message}. Retrying...`);
      continue;
    }

    console.log(`[RWA-JOB]   Poll ${i + 1}/90: state=${st.status.state}`);
    if (st.status.state === 'DONE') {
      if (st.status.errorResult) {
        const err = st.status.errorResult;
        throw new Error(`${err.message} (reason: ${err.reason || 'n/a'}, location: ${err.location || 'n/a'})`);
      }
      console.log('[RWA-JOB] BQ job DONE ✔');
      return;
    }
  }

  throw new Error(`BigQuery job "${jobId}" did not complete within the timeout window.`);
}