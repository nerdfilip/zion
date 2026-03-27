// ============================================================================
// CONFIGURATION: EXPORT PT QUERY PIPELINE
// ============================================================================
const EPT_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b';
const EPT_DATASET_ID = 'imports';
const EPT_TABLE_ID   = 'export_pt';
const EPT_AUTO_CREATE_TARGET_TABLE = true; // Create once if missing, never recreate each run
// Write strategy for existing target table:
//   - 'MERGE': sync existing table (update, insert, delete missing) [recommended]
//   - 'TRUNCATE_INSERT': full refresh by truncating and inserting
//   - 'APPEND': insert only
const EPT_WRITE_MODE = 'MERGE';

// ============================================================================
// UI TRIGGER
// ============================================================================
function openTransformationsUI() {
  const html = HtmlService.createHtmlOutputFromFile('TransformationsUI')
    .setWidth(700)
    .setHeight(560)
    .setTitle('Export PT → BigQuery');
  SpreadsheetApp.getUi().showModalDialog(html, 'Export PT Query');
}

// ============================================================================
// RUN EXPORT PT POWER QUERY LOGIC DIRECTLY IN BIGQUERY
//
// Source: existing raw tables from BigQueryLoader (raw_export_pt_*)
// Target: existing native table EPT_TABLE_ID
//
// Power Query mapping:
//   • Folder.Files + combine files   => UNION ALL raw_export_pt_* tables
//   • Filter stock values            => WHERE stock NOT IN ('0', 'Stock', 'NOT_DISPLAY', '')
//   • Rename/split source name       => country code from table suffix (e.g. raw_export_pt_be -> BE)
//   • Group by warehouse + article   => GROUP BY virtuelles_warenhaus, artikelnummer
// ============================================================================
function runExportPTPowerQuery() {
  try {
    console.log('[EPT-BQ] Starting Export PT Power Query transformation...');

    _ensureExportPTTargetTable_();

    const sourceTables = _getExportPTSourceTables_();
    if (sourceTables.length === 0) {
      return {
        success: false,
        log: '[ERROR] No source tables found. Expected naming: raw_export_pt_<country> (e.g. raw_export_pt_be, raw_export_pt_de).'
      };
    }

    const sourceSpecs = _buildExportPTSourceSpecs_(sourceTables);
    if (!sourceSpecs.length) {
      return {
        success: false,
        log: '[ERROR] No usable source tables found. Required columns: article_number (or alias) and stock (or alias).'
      };
    }

    console.log(`[EPT-BQ] Usable source tables (${sourceSpecs.length}): ${sourceSpecs.map(s => s.tableName).join(', ')}`);

    const unionSql = sourceSpecs.map(spec => {
      const tableName = spec.tableName;
      const warehouse = tableName.replace(/^raw_export_pt_/, '').toUpperCase();
      const articleCol = spec.articleCol;
      const ianCol = spec.ianCol;
      const stockCol = spec.stockCol;
      return `
        SELECT
          '${warehouse}' AS virtuelles_warenhaus,
          CAST(\`${articleCol}\` AS STRING) AS article_raw,
          CAST(\`${ianCol}\` AS STRING) AS ian_raw,
          CAST(\`${stockCol}\` AS STRING) AS stock_raw
        FROM \`${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${tableName}\`
      `.trim();
    }).join('\nUNION ALL\n');

    const diagnosticsSql = `
      WITH unified AS (
        ${unionSql}
      ),
      filtered AS (
        SELECT *
        FROM unified
        WHERE (
          stock_raw IS NULL
          OR TRIM(stock_raw) = ''
          OR NOT (
            UPPER(TRIM(stock_raw)) IN ('STOCK', 'NOT_DISPLAY')
            OR TRIM(stock_raw) = '0'
            OR SAFE_CAST(TRIM(stock_raw) AS INT64) = 0
          )
        )
      ),
      typed AS (
        SELECT
          virtuelles_warenhaus,
          SAFE_CAST(NULLIF(TRIM(article_raw), '') AS INT64) AS artikelnummer,
          SAFE_CAST(NULLIF(TRIM(stock_raw), '') AS INT64) AS bestand
        FROM filtered
      ),
      grouped AS (
        SELECT
          virtuelles_warenhaus,
          artikelnummer,
          NULLIF(SUM(bestand), 0) AS bestand
        FROM typed
        WHERE artikelnummer IS NOT NULL
        GROUP BY virtuelles_warenhaus, artikelnummer
      ),
      grouped_counts AS (
        SELECT virtuelles_warenhaus, COUNT(1) AS grouped_rows
        FROM grouped
        GROUP BY virtuelles_warenhaus
      )
      SELECT
        t.virtuelles_warenhaus,
        COUNT(1) AS rows_after_filter,
        COUNTIF(t.artikelnummer IS NULL) AS null_article_rows,
        COUNTIF(t.bestand IS NULL) AS null_stock_rows,
        COUNTIF(t.artikelnummer IS NOT NULL AND t.bestand IS NOT NULL) AS fully_typed_rows,
        IFNULL(MAX(gc.grouped_rows), 0) AS grouped_rows
      FROM typed t
      LEFT JOIN grouped_counts gc
        ON gc.virtuelles_warenhaus = t.virtuelles_warenhaus
      GROUP BY virtuelles_warenhaus
      ORDER BY virtuelles_warenhaus
    `;
    _logExportPTDiagnostics_(diagnosticsSql);

    const groupedSourceSql = `
      WITH unified AS (
        ${unionSql}
      ),
      filtered AS (
        SELECT *
        FROM unified
        WHERE (
          stock_raw IS NULL
          OR TRIM(stock_raw) = ''
          OR NOT (
            UPPER(TRIM(stock_raw)) IN ('STOCK', 'NOT_DISPLAY')
            OR TRIM(stock_raw) = '0'
            OR SAFE_CAST(TRIM(stock_raw) AS INT64) = 0
          )
        )
      ),
      typed AS (
        SELECT
          virtuelles_warenhaus,
          SAFE_CAST(NULLIF(TRIM(article_raw), '') AS INT64) AS artikelnummer,
          SAFE_CAST(NULLIF(TRIM(stock_raw), '') AS INT64) AS bestand
        FROM filtered
      )
      SELECT
        virtuelles_warenhaus,
        artikelnummer,
        NULLIF(SUM(bestand), 0) AS bestand
      FROM typed
      WHERE artikelnummer IS NOT NULL
      GROUP BY virtuelles_warenhaus, artikelnummer
    `;

    if (EPT_WRITE_MODE === 'TRUNCATE_INSERT') {
      console.log('[EPT-BQ] Write mode TRUNCATE_INSERT: replacing full target contents.');
      _eptRunBQJob(`TRUNCATE TABLE \`${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${EPT_TABLE_ID}\``);
      _eptRunBQJob(`
        INSERT INTO \`${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${EPT_TABLE_ID}\` (virtuelles_warenhaus, artikelnummer, bestand)
        ${groupedSourceSql}
      `);
    } else if (EPT_WRITE_MODE === 'APPEND') {
      console.log('[EPT-BQ] Write mode APPEND: adding grouped rows only.');
      _eptRunBQJob(`
        INSERT INTO \`${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${EPT_TABLE_ID}\` (virtuelles_warenhaus, artikelnummer, bestand)
        ${groupedSourceSql}
      `);
    } else {
      console.log('[EPT-BQ] Write mode MERGE: syncing existing target table (update/insert/delete).');
      _eptRunBQJob(`
        MERGE \`${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${EPT_TABLE_ID}\` T
        USING (
          ${groupedSourceSql}
        ) S
        ON T.virtuelles_warenhaus = S.virtuelles_warenhaus
           AND T.artikelnummer = S.artikelnummer
        WHEN MATCHED THEN
          UPDATE SET T.bestand = S.bestand
        WHEN NOT MATCHED THEN
          INSERT (virtuelles_warenhaus, artikelnummer, bestand)
          VALUES (S.virtuelles_warenhaus, S.artikelnummer, S.bestand)
        WHEN NOT MATCHED BY SOURCE THEN
          DELETE
      `);
    }

    const countSql = `
      SELECT COUNT(1) AS row_count
      FROM \`${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${EPT_TABLE_ID}\`
    `;
    const rowCount = _eptQuerySingleCount_(countSql);

    return {
      success: true,
      rowCount: rowCount,
      log: `[SUCCESS] Export PT logic executed in BigQuery. Source tables: ${sourceTables.length}. Target rows now: ${rowCount}.`
    };

  } catch (e) {
    console.error(`[EPT-BQ] Fatal: ${e.message}`);
    console.error(`[EPT-BQ] Stack trace:\n${e.stack}`);
    return { success: false, log: `[CRITICAL] Export PT query failed: ${e.message}` };
  }
}

function _buildExportPTSourceSpecs_(sourceTables) {
  const specs = [];

  sourceTables.forEach(tableName => {
    try {
      const table = BigQuery.Tables.get(EPT_PROJECT_ID, EPT_DATASET_ID, tableName);
      const fields = ((table.schema && table.schema.fields) || []).map(f => String(f.name || '').toLowerCase());

      const articleCol = _pickFirstColumn_(fields, ['article_number', 'article_number_1', 'artikelnummer', 'artikelnummer_1']);
      const ianCol = _pickFirstColumn_(fields, ['ian', 'ian_1', 'ean', 'ean_1']) || 'ian';
      const stockCol = _pickFirstColumn_(fields, ['stock', 'bestand', 'best', 'stock_1']);

      if (!articleCol || !stockCol) {
        console.log(`[EPT-BQ] Skipping table ${tableName}: missing required columns (article=${articleCol || 'n/a'}, stock=${stockCol || 'n/a'}).`);
        return;
      }

      // If ian is missing in a source table, keep compatibility by selecting NULL AS ian_raw later.
      const resolvedIanCol = fields.indexOf(ianCol) !== -1 ? ianCol : null;

      specs.push({ tableName: tableName, articleCol: articleCol, ianCol: resolvedIanCol || articleCol, stockCol: stockCol });
      console.log(`[EPT-BQ] Table ${tableName}: article=${articleCol}, ian=${resolvedIanCol || 'n/a'}, stock=${stockCol}`);
    } catch (e) {
      console.log(`[EPT-BQ] Skipping table ${tableName}: schema read failed (${e.message}).`);
    }
  });

  return specs;
}

function _pickFirstColumn_(fieldNames, candidates) {
  for (let i = 0; i < candidates.length; i++) {
    const c = candidates[i];
    if (fieldNames.indexOf(c) !== -1) return c;
  }
  return null;
}

function _logExportPTDiagnostics_(sql) {
  const req = { query: sql, useLegacySql: false };
  const res = BigQuery.Jobs.query(req, EPT_PROJECT_ID);
  const rows = res.rows || [];
  if (!rows.length) {
    console.log('[EPT-BQ] Diagnostics: no rows returned.');
    return;
  }

  console.log('[EPT-BQ] Diagnostics by warehouse (after PQ stock filter):');
  rows.forEach(r => {
    const f = r.f || [];
    const warehouse = f[0] && f[0].v;
    const afterFilter = f[1] && f[1].v;
    const nullArticle = f[2] && f[2].v;
    const nullStock = f[3] && f[3].v;
    const fullyTyped = f[4] && f[4].v;
    console.log(`[EPT-BQ]   ${warehouse}: rows=${afterFilter}, nullArticle=${nullArticle}, nullStock=${nullStock}, fullyTyped=${fullyTyped}`);
  });
}

function _ensureExportPTTargetTable_() {
  try {
    BigQuery.Tables.get(EPT_PROJECT_ID, EPT_DATASET_ID, EPT_TABLE_ID);
    console.log(`[EPT-BQ] Target table exists: ${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${EPT_TABLE_ID}`);
    return;
  } catch (e) {
    if (!EPT_AUTO_CREATE_TARGET_TABLE) {
      throw new Error(
        `Target table does not exist: ${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${EPT_TABLE_ID}. ` +
        `Please create it first with columns: virtuelles_warenhaus STRING, artikelnummer INT64, bestand INT64.`
      );
    }
  }

  console.log(`[EPT-BQ] Target table missing. Auto-creating ${EPT_PROJECT_ID}.${EPT_DATASET_ID}.${EPT_TABLE_ID}...`);
  const tableResource = {
    tableReference: {
      projectId: EPT_PROJECT_ID,
      datasetId: EPT_DATASET_ID,
      tableId: EPT_TABLE_ID
    },
    schema: {
      fields: [
        { name: 'virtuelles_warenhaus', type: 'STRING' },
        { name: 'artikelnummer', type: 'INT64' },
        { name: 'bestand', type: 'INT64' }
      ]
    }
  };

  BigQuery.Tables.insert(tableResource, EPT_PROJECT_ID, EPT_DATASET_ID);
  console.log('[EPT-BQ] Target table created successfully.');
}

function _getExportPTSourceTables_() {
  const pattern = /^raw_export_pt_[a-z]{2,3}$/;
  let pageToken;
  const matches = [];

  do {
    const list = BigQuery.Tables.list(EPT_PROJECT_ID, EPT_DATASET_ID, { maxResults: 1000, pageToken: pageToken });
    const tables = list.tables || [];
    tables.forEach(t => {
      const tableId = t.tableReference && t.tableReference.tableId;
      if (tableId && pattern.test(tableId)) {
        matches.push(tableId);
      } else if (tableId && /^raw_export_pt_/i.test(tableId)) {
        console.log(`[EPT-BQ] Skipping non-country source table: ${tableId}`);
      }
    });
    pageToken = list.nextPageToken;
  } while (pageToken);

  return matches.sort();
}

function _eptQuerySingleCount_(sql) {
  const req = { query: sql, useLegacySql: false };
  const res = BigQuery.Jobs.query(req, EPT_PROJECT_ID);
  const rows = res.rows || [];
  if (!rows.length) return 0;
  const raw = rows[0].f && rows[0].f[0] && rows[0].f[0].v;
  return Number(raw || 0);
}

// ============================================================================
// INTERNAL HELPER — Submit a BigQuery SQL job and poll until completion
// ============================================================================
function _eptRunBQJob(sql) {
  const preview = sql.replace(/\s+/g, ' ').trim().substring(0, 200);
  console.log(`[JOB] Submitting SQL: ${preview}...`);

  const job      = BigQuery.Jobs.insert(
    { configuration: { query: { query: sql, useLegacySql: false } } },
    EPT_PROJECT_ID
  );
  const jobId    = job.jobReference.jobId;
  const location = job.jobReference.location;
  console.log(`[JOB] Job submitted → jobId: ${jobId} | location: ${location}`);

  for (let i = 0; i < 90; i++) {
    Utilities.sleep(2000);
    let st;
    try {
      st = BigQuery.Jobs.get(EPT_PROJECT_ID, jobId, { location });
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
