// ============================================================================
// EXECUTE STORED PROCEDURES PIPELINE
// ============================================================================
const EQ_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b';
const EQ_DATASET_ID = 'staging';
const EQ_OUTPUT_TABLE_ID = 'lagerliste_komplett';

// Stored procedures to execute in order
const EQ_PROCEDURES = [
  {
    name: 'sp_build_export_pt',
    label: 'Export PT',
    call: `CALL \`${EQ_PROJECT_ID}.${EQ_DATASET_ID}.sp_export_pt\`()`
  },
  {
    name: 'sp_build_rwa_pq',
    label: 'RWA PQ',
    call: `CALL \`${EQ_PROJECT_ID}.${EQ_DATASET_ID}.sp_rwa_pq\`()`
  },
  {
    name: 'sp_aktionsplan_int_pq',
    label: 'Aktionsplan INT PQ',
    call: `CALL \`${EQ_PROJECT_ID}.${EQ_DATASET_ID}.sp_aktionsplan_int_pq\`()`
  },
  {
    name: 'sp_lagerliste_komplett',
    label: 'Lagerliste Komplett',
    call: `CALL \`${EQ_PROJECT_ID}.${EQ_DATASET_ID}.sp_lagerliste_komplett\`()`
  }
];

const EQ_FOLDER_CELL_MAP = {
  uploads: 'B2',
  ready: 'B3',
  archive: 'B4',
  output: 'B5'
};

// ============================================================================
// UI TRIGGER
// ============================================================================
function openExecuteQueriesUI() {
  const html = HtmlService.createHtmlOutputFromFile('ExecuteQueriesUI')
    .setWidth(700)
    .setHeight(620)
    .setTitle('Execute Stored Procedures');
  SpreadsheetApp.getUi().showModalDialog(html, 'Execute Stored Procedures');
}

// ============================================================================
// FOLDER CONFIG FROM FIRST SHEET (B2:B5)
// ============================================================================
function getPipelineFolderConfig() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const firstSheet = ss.getSheets()[0];
  if (!firstSheet) {
    throw new Error('No sheets found in the active spreadsheet.');
  }

  const cfg = {
    uploads: resolveFolderFromCell_(firstSheet.getRange(EQ_FOLDER_CELL_MAP.uploads).getValue(), '01_Uploads'),
    ready: resolveFolderFromCell_(firstSheet.getRange(EQ_FOLDER_CELL_MAP.ready).getValue(), '02_Ready'),
    archive: resolveFolderFromCell_(firstSheet.getRange(EQ_FOLDER_CELL_MAP.archive).getValue(), '03_Archive'),
    output: resolveFolderFromCell_(firstSheet.getRange(EQ_FOLDER_CELL_MAP.output).getValue(), '04_Output')
  };

  return {
    uploads: { id: cfg.uploads.getId(), name: cfg.uploads.getName() },
    ready: { id: cfg.ready.getId(), name: cfg.ready.getName() },
    archive: { id: cfg.archive.getId(), name: cfg.archive.getName() },
    output: { id: cfg.output.getId(), name: cfg.output.getName() }
  };
}

function resolveFolderFromCell_(rawValue, label) {
  const value = String(rawValue || '').trim();
  if (!value) {
    throw new Error(`Missing folder value for ${label}. Please fill ${label} in the first sheet.`);
  }

  const idFromUrl = extractDriveFolderId_(value);
  if (idFromUrl) {
    return DriveApp.getFolderById(idFromUrl);
  }

  if (/^[A-Za-z0-9_-]{20,}$/.test(value)) {
    return DriveApp.getFolderById(value);
  }

  const byName = DriveApp.getFoldersByName(value);
  if (byName.hasNext()) {
    return byName.next();
  }

  throw new Error(`Could not resolve folder for ${label} from value: ${value}`);
}

function extractDriveFolderId_(value) {
  const byUrl = value.match(/\/folders\/([a-zA-Z0-9_-]+)/);
  if (byUrl && byUrl[1]) {
    return byUrl[1];
  }

  const byResourceKey = value.match(/id=([a-zA-Z0-9_-]+)/);
  if (byResourceKey && byResourceKey[1]) {
    return byResourceKey[1];
  }

  return null;
}

// ============================================================================
// EXECUTE A SINGLE STORED PROCEDURE
// ============================================================================
function executeStoredProcedure(index) {
  const proc = EQ_PROCEDURES[index];
  if (!proc) {
    return { success: false, log: `[ERROR] Invalid procedure index: ${index}` };
  }

  try {
    console.log(`[EQ] Executing stored procedure: ${proc.label} (${proc.name})...`);

    const job = BigQuery.Jobs.insert(
      { configuration: { query: { query: proc.call, useLegacySql: false } } },
      EQ_PROJECT_ID
    );
    const jobId    = job.jobReference.jobId;
    const location = job.jobReference.location;
    console.log(`[EQ] Job submitted → jobId: ${jobId} | location: ${location}`);

    // Poll until the job completes (up to 90 × 2s = 3 min)
    for (let i = 0; i < 90; i++) {
      Utilities.sleep(2000);
      let st;
      try {
        st = BigQuery.Jobs.get(EQ_PROJECT_ID, jobId, { location });
      } catch (pollErr) {
        console.warn(`[EQ] Poll ${i + 1}/90: API hiccup — ${pollErr.message}. Retrying...`);
        continue;
      }
      const state = st.status.state;
      console.log(`[EQ] Poll ${i + 1}/90: state=${state}`);
      if (state === 'DONE') {
        if (st.status.errorResult) {
          const errMsg = st.status.errorResult.message || JSON.stringify(st.status.errorResult);
          console.error(`[EQ] Procedure ${proc.name} failed: ${errMsg}`);
          return { success: false, log: `[ERROR] ${proc.label} failed: ${errMsg}` };
        }
        console.log(`[EQ] Stored procedure ${proc.name} completed successfully.`);
        return {
          success: true,
          log: `[SUCCESS] ${proc.label} (${proc.name}) executed successfully.`
        };
      }
    }

    return { success: false, log: `[ERROR] ${proc.label} timed out after 3 minutes.` };

  } catch (e) {
    console.error(`[EQ] Failed to execute ${proc.name}: ${e.message}`);
    return {
      success: false,
      log: `[CRITICAL] ${proc.label} failed: ${e.message}`
    };
  }
}

// ============================================================================
// START + POLL API FOR REAL-TIME UI
// ============================================================================
function startStoredProcedure(index) {
  const proc = EQ_PROCEDURES[index];
  if (!proc) {
    return { success: false, log: `[ERROR] Invalid procedure index: ${index}` };
  }

  const MAX_START_ATTEMPTS = 4;
  let lastErr;

  for (let attempt = 1; attempt <= MAX_START_ATTEMPTS; attempt++) {
    try {
      console.log(`[EQ] Starting ${proc.label} (attempt ${attempt})...`);

      const job = BigQuery.Jobs.insert(
        { configuration: { query: { query: proc.call, useLegacySql: false } } },
        EQ_PROJECT_ID
      );

      const jobId = job.jobReference.jobId;
      const location = job.jobReference.location;
      return {
        success: true,
        label: proc.label,
        name: proc.name,
        jobId,
        location,
        log: `[STARTED] ${proc.label} (${proc.name})`
      };
    } catch (e) {
      lastErr = e;
      console.warn(`[EQ] Start attempt ${attempt}/${MAX_START_ATTEMPTS} failed for ${proc.label}: ${e.message}`);
      if (attempt < MAX_START_ATTEMPTS) {
        Utilities.sleep(2000 * attempt);
      }
    }
  }

  return {
    success: false,
    log: `[CRITICAL] Failed to start ${proc.label} after ${MAX_START_ATTEMPTS} attempts: ${lastErr.message}`
  };
}

function pollStoredProcedure(index, jobId, location) {
  const proc = EQ_PROCEDURES[index];
  if (!proc) {
    return { success: false, done: true, log: `[ERROR] Invalid procedure index: ${index}` };
  }

  if (!jobId || !location) {
    return { success: false, done: true, log: `[ERROR] Missing job metadata for ${proc.label}.` };
  }

  try {
    const st = BigQuery.Jobs.get(EQ_PROJECT_ID, jobId, { location });
    const state = st.status && st.status.state ? st.status.state : 'UNKNOWN';

    if (state !== 'DONE') {
      return {
        success: true,
        done: false,
        state,
        log: `[RUNNING] ${proc.label}: state=${state}`
      };
    }

    if (st.status.errorResult) {
      const errMsg = st.status.errorResult.message || JSON.stringify(st.status.errorResult);
      return {
        success: false,
        done: true,
        state,
        log: `[ERROR] ${proc.label} failed: ${errMsg}`
      };
    }

    return {
      success: true,
      done: true,
      state,
      log: `[SUCCESS] ${proc.label} (${proc.name}) executed successfully.`
    };
  } catch (e) {
    // Some polling calls can fail transiently (e.g. temporary empty responses).
    // Keep polling instead of failing the whole pipeline.
    return {
      success: true,
      done: false,
      state: 'RUNNING',
      transient: true,
      log: `[WARN] Poll retry for ${proc.label}: ${e.message}`
    };
  }
}

// ============================================================================
// CONNECTED SHEET GENERATION IN 04_OUTPUT
// ============================================================================
function createLagerlisteConnectedSheet() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    SpreadsheetApp.enableBigQueryExecution();
    const removedCount = removeExistingLagerlisteConnectedSheets_(ss, EQ_OUTPUT_TABLE_ID);
    const source = resolveConnectedSheetSource_();

    const spec = SpreadsheetApp.newDataSourceSpec()
      .asBigQuery()
      .setProjectId(EQ_PROJECT_ID)
      .setTableProjectId(EQ_PROJECT_ID)
      .setDatasetId(source.datasetId)
      .setTableId(source.tableId)
      .build();

    const dataSourceSheet = ss.insertDataSourceSheet(spec);
    const createdSheet = resolveSheetFromDataSource_(ss, dataSourceSheet);
    const sheetName = EQ_OUTPUT_TABLE_ID;
    if (createdSheet) {
      createdSheet.setName(sheetName);
    }
    dataSourceSheet.refreshData();

    return {
      success: true,
      spreadsheetId: ss.getId(),
      spreadsheetUrl: ss.getUrl(),
      sheetName,
      replacedSheets: removedCount,
      sourceDatasetId: source.datasetId,
      sourceTableId: source.tableId,
      log: `[SUCCESS] Connected Sheet recreated in current spreadsheet: ${sheetName}`
    };
  } catch (e) {
    return {
      success: false,
      log: `[ERROR] Failed to create Connected Sheet: ${e.message}`
    };
  }
}

function removeExistingLagerlisteConnectedSheets_(spreadsheet, baseName) {
  const sheets = spreadsheet.getSheets();
  const toDelete = sheets.filter(function (s) {
    const name = s.getName();
    return name === baseName || name.indexOf(baseName + '_') === 0;
  });

  toDelete.forEach(function (sheet) {
    spreadsheet.deleteSheet(sheet);
  });

  return toDelete.length;
}

function resolveConnectedSheetSource_() {
  try {
    BigQuery.Tables.get(EQ_PROJECT_ID, EQ_DATASET_ID, EQ_OUTPUT_TABLE_ID);
    return { datasetId: EQ_DATASET_ID, tableId: EQ_OUTPUT_TABLE_ID };
  } catch (e) {
    throw new Error(
      `Connected Sheet source not found: ${EQ_PROJECT_ID}.${EQ_DATASET_ID}.${EQ_OUTPUT_TABLE_ID}. ${e.message}`
    );
  }
}

function resolveSheetFromDataSource_(spreadsheet, dataSourceSheet) {
  if (!dataSourceSheet) {
    return null;
  }

  // `insertDataSourceSheet` returns DataSourceSheet; rename must run on Sheet.
  if (typeof dataSourceSheet.getSheet === 'function') {
    return dataSourceSheet.getSheet();
  }

  if (typeof dataSourceSheet.getSheetId === 'function') {
    const targetId = dataSourceSheet.getSheetId();
    const match = spreadsheet.getSheets().find(function (s) {
      return s.getSheetId() === targetId;
    });
    return match || null;
  }

  return null;
}

function getExecuteQueriesConfig() {
  return {
    totalProcedures: EQ_PROCEDURES.length,
    procedureLabels: EQ_PROCEDURES.map(function (p) { return p.label; })
  };
}

// ============================================================================
// GET TOTAL PROCEDURE COUNT (for UI)
// ============================================================================
function getStoredProcedureCount() {
  return EQ_PROCEDURES.length;
}