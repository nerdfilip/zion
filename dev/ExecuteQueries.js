// ============================================================================
// EXECUTE STORED PROCEDURES PIPELINE
// ============================================================================
const EQ_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b';
const EQ_DATASET_ID = 'staging';
const EQ_OUTPUT_TABLE_ID = 'lagerliste_komplett';
const EQ_OUTPUT_SHEET_NAME = 'Lagerliste Komplett';

// Stored procedures to execute in order
const EQ_PROCEDURES = [
  {
    name: 'sp_export_pt',
    label: 'Export PT',
    call: `CALL \`${EQ_PROJECT_ID}.${EQ_DATASET_ID}.sp_export_pt\`()`
  },
  {
    name: 'sp_rwa_pq',
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
  },
  {
    name: 'sp_lagerliste_v3',
    label: 'Lagerliste V3',
    call: `CALL \`${EQ_PROJECT_ID}.${EQ_DATASET_ID}.sp_lagerliste_v3\`()`
  }
];

// ============================================================================
// UI TRIGGER
// ============================================================================
function openExecuteQueriesUI() {
  const html = HtmlService.createHtmlOutputFromFile('ExecuteQueriesUI')
    .setWidth(600)
    .setHeight(500)
    .setTitle('Execute Stored Procedures');
  SpreadsheetApp.getUi().showModalDialog(html, 'Execute Stored Procedures');
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
    const removedCount = removeExistingLagerlisteConnectedSheets_(ss, EQ_OUTPUT_SHEET_NAME);
    const source = resolveConnectedSheetSource_();

    const spec = SpreadsheetApp.newDataSourceSpec()
      .asBigQuery()
      .setProjectId(EQ_PROJECT_ID)
      .setTableProjectId(EQ_PROJECT_ID)
      .setDatasetId(source.datasetId)
      .setTableId(source.tableId)
      .build();

    const dataSourceSheet = ss.insertDataSourceSheet(spec);
    const createdSheet = resolveSheetFromDataSource_(ss, dataSourceSheet) || findCreatedConnectedSheet_(ss);
    const sheetName = EQ_OUTPUT_SHEET_NAME;
    if (!createdSheet) {
      throw new Error('Could not resolve created Connected Sheet for renaming.');
    }
    createdSheet.setName(sheetName);
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
    return name === baseName || name === EQ_OUTPUT_TABLE_ID || name.indexOf(baseName + '_') === 0 || name.indexOf(EQ_OUTPUT_TABLE_ID + '_') === 0;
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

function findCreatedConnectedSheet_(spreadsheet) {
  const sheets = spreadsheet.getSheets();
  if (!sheets || sheets.length === 0) {
    return null;
  }

  const byTableName = sheets.find(function (s) {
    return s.getName() === EQ_OUTPUT_TABLE_ID;
  });
  if (byTableName) {
    return byTableName;
  }

  return sheets[sheets.length - 1] || null;
}

function getExecuteQueriesConfig() {
  return {
    totalProcedures: EQ_PROCEDURES.length,
    procedureLabels: EQ_PROCEDURES.map(function (p) { return p.label; }),
    procedureNames: EQ_PROCEDURES.map(function (p) { return p.name; }),
    includeLegacyConnectedSheetStep: true,
    includeNachbetrachtungConnectedSheetStep: false,
    connectedSheetCountries: [],
    connectedSheetTableIds: []
  };
}

// ============================================================================
// GET TOTAL PROCEDURE COUNT (for UI)
// ============================================================================
function getStoredProcedureCount() {
  return EQ_PROCEDURES.length;
}
