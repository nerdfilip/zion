// ============================================================================
// EXECUTE STORED PROCEDURES PIPELINE
// ============================================================================
const EQ_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b';
const EQ_DATASET_ID = 'staging';
const EQ_OUTPUT_TABLE_ID = 'lagerliste_komplett';
const EQ_OUTPUT_SHEET_NAME = 'Lagerliste Komplett';
const EQ_V3_SPREADSHEET_NAME = 'Lagerliste_V3';
const EQ_COUNTRY_CONNECTED_SHEETS = [
  { country: 'AT', tableId: 'Lagerliste_AT' },
  { country: 'BE', tableId: 'Lagerliste_BE' },
  { country: 'CZ', tableId: 'Lagerliste_CZ' },
  { country: 'DE', tableId: 'Lagerliste_DE' },
  { country: 'ES', tableId: 'Lagerliste_ES' },
  { country: 'FR', tableId: 'Lagerliste_FR' },
  { country: 'INT', tableId: 'Lagerliste_INT' },
  { country: 'NL', tableId: 'Lagerliste_NL' },
  { country: 'PL', tableId: 'Lagerliste_PL' },
  { country: 'SK', tableId: 'Lagerliste_SK' }
];
const EQ_CONNECTED_SHEET_MAX_ATTEMPTS = 4;

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
    connectedSheetCountries: EQ_COUNTRY_CONNECTED_SHEETS.map(function (entry) { return entry.country; }),
    connectedSheetTableIds: EQ_COUNTRY_CONNECTED_SHEETS.map(function (entry) { return entry.tableId; })
  };
}

// ============================================================================
// GET TOTAL PROCEDURE COUNT (for UI)
// ============================================================================
function getStoredProcedureCount() {
  return EQ_PROCEDURES.length;
}

// ============================================================================
// CONNECTED SHEETS V3 GENERATION (NEW SPREADSHEET + REAL-TIME POLLING)
// ============================================================================
function resolveOutputFolder_() {
  const firstSheet = SpreadsheetApp.getActiveSpreadsheet().getSheets()[0];
  if (!firstSheet) {
    throw new Error('Cannot read folder config: no sheets found.');
  }

  const raw = firstSheet.getRange(EQ_FOLDER_CELL_MAP.output).getValue();
  return resolveFolderFromCell_(raw, '04_Output');
}

function deleteExistingLagerlisteV3_(outputFolder) {
  const files = outputFolder.getFilesByName(EQ_V3_SPREADSHEET_NAME);
  let deletedCount = 0;

  while (files.hasNext()) {
    const file = files.next();
    file.setTrashed(true);
    deletedCount += 1;
  }

  return deletedCount;
}

function moveFileToFolder_(fileId, targetFolder) {
  const file = DriveApp.getFileById(fileId);

  // Remove from all current parent folders, add to target.
  const parents = file.getParents();
  while (parents.hasNext()) {
    parents.next().removeFile(file);
  }

  targetFolder.addFile(file);
}

function startCreateLagerlisteV3ConnectedSheets() {
  try {
    SpreadsheetApp.enableBigQueryExecution();

    const outputFolder = resolveOutputFolder_();
    const deletedCount = deleteExistingLagerlisteV3_(outputFolder);
    console.log(`[EQ] Removed ${deletedCount} existing Lagerliste_V3 file(s) from output folder.`);

    const ss = SpreadsheetApp.create(EQ_V3_SPREADSHEET_NAME);
    moveFileToFolder_(ss.getId(), outputFolder);

    const firstSheet = ss.getSheets()[0];
    const operationId = Utilities.getUuid();

    const state = {
      operationId: operationId,
      spreadsheetId: ss.getId(),
      spreadsheetUrl: ss.getUrl(),
      spreadsheetName: EQ_V3_SPREADSHEET_NAME,
      defaultSheetId: firstSheet ? firstSheet.getSheetId() : null,
      nextIndex: 0,
      totalTables: EQ_COUNTRY_CONNECTED_SHEETS.length,
      createdCountries: [],
      tableAttempts: {}
    };

    saveConnectedSheetOperationState_(operationId, state);

    return {
      success: true,
      operationId: operationId,
      spreadsheetId: state.spreadsheetId,
      spreadsheetUrl: state.spreadsheetUrl,
      spreadsheetName: state.spreadsheetName,
      totalTables: state.totalTables,
      log: `[STARTED] Connected Sheets creation started in ${state.spreadsheetName}.`
    };
  } catch (e) {
    return {
      success: false,
      done: true,
      log: `[ERROR] Failed to start Connected Sheets generation: ${e.message}`
    };
  }
}

function pollCreateLagerlisteV3ConnectedSheets(operationId) {
  if (!operationId) {
    return { success: false, done: true, log: '[ERROR] Missing Connected Sheets operation ID.' };
  }

  // Each Apps Script execution context must explicitly enable BigQuery data execution.
  SpreadsheetApp.enableBigQueryExecution();

  const state = loadConnectedSheetOperationState_(operationId);
  if (!state) {
    return {
      success: false,
      done: true,
      log: '[ERROR] Connected Sheets operation state not found or expired. Please restart the pipeline.'
    };
  }

  try {
    if (state.nextIndex >= state.totalTables) {
      finalizeConnectedSheetSpreadsheet_(state);
      clearConnectedSheetOperationState_(operationId);
      return {
        success: true,
        done: true,
        completedTables: state.totalTables,
        totalTables: state.totalTables,
        spreadsheetId: state.spreadsheetId,
        spreadsheetUrl: state.spreadsheetUrl,
        spreadsheetName: state.spreadsheetName,
        createdCountries: state.createdCountries || [],
        log: `[SUCCESS] Connected Sheets completed in ${state.spreadsheetName} (${state.totalTables}/${state.totalTables}).`
      };
    }

    const entry = EQ_COUNTRY_CONNECTED_SHEETS[state.nextIndex];
    state.tableAttempts = state.tableAttempts || {};
    const currentAttempt = (state.tableAttempts[entry.tableId] || 0) + 1;
    state.tableAttempts[entry.tableId] = currentAttempt;

    const source = resolveConnectedSheetSourceByTable_(entry.tableId);
    const ss = SpreadsheetApp.openById(state.spreadsheetId);

    const existingSheet = ss.getSheetByName(entry.country);
    if (existingSheet) {
      state.nextIndex += 1;
      state.createdCountries = state.createdCountries || [];
      if (state.createdCountries.indexOf(entry.country) === -1) {
        state.createdCountries.push(entry.country);
      }
      delete state.tableAttempts[entry.tableId];
      saveConnectedSheetOperationState_(operationId, state);

      return {
        success: true,
        done: false,
        completedTables: state.nextIndex,
        totalTables: state.totalTables,
        currentCountry: entry.country,
        currentTableId: entry.tableId,
        spreadsheetId: state.spreadsheetId,
        spreadsheetUrl: state.spreadsheetUrl,
        spreadsheetName: state.spreadsheetName,
        log: `[INFO] Connected Sheet ${entry.country} already exists; continuing (${state.nextIndex}/${state.totalTables}).`
      };
    }

    const spec = SpreadsheetApp.newDataSourceSpec()
      .asBigQuery()
      .setProjectId(EQ_PROJECT_ID)
      .setTableProjectId(EQ_PROJECT_ID)
      .setDatasetId(source.datasetId)
      .setTableId(source.tableId)
      .build();

    const dataSourceSheet = ss.insertDataSourceSheet(spec);
    const createdSheet = resolveSheetFromDataSource_(ss, dataSourceSheet) || findCreatedConnectedSheet_(ss);
    if (!createdSheet) {
      throw new Error(`Could not resolve created sheet for ${entry.country}.`);
    }

    createdSheet.setName(entry.country);
    dataSourceSheet.refreshData();

    state.nextIndex += 1;
    state.createdCountries = state.createdCountries || [];
    state.createdCountries.push(entry.country);
    delete state.tableAttempts[entry.tableId];
    saveConnectedSheetOperationState_(operationId, state);

    return {
      success: true,
      done: false,
      completedTables: state.nextIndex,
      totalTables: state.totalTables,
      currentCountry: entry.country,
      currentTableId: entry.tableId,
      spreadsheetId: state.spreadsheetId,
      spreadsheetUrl: state.spreadsheetUrl,
      spreadsheetName: state.spreadsheetName,
      log: `[SUCCESS] Connected Sheet ${entry.country} created from ${source.datasetId}.${source.tableId} (${state.nextIndex}/${state.totalTables}).`
    };
  } catch (e) {
    const retryEntry = EQ_COUNTRY_CONNECTED_SHEETS[state.nextIndex];
    const retryTableId = retryEntry ? retryEntry.tableId : null;
    const retryAttempt = retryTableId && state.tableAttempts ? state.tableAttempts[retryTableId] : 1;

    if (isTransientBigQueryError_(e) && retryAttempt < EQ_CONNECTED_SHEET_MAX_ATTEMPTS) {
      saveConnectedSheetOperationState_(operationId, state);
      return {
        success: true,
        done: false,
        transient: true,
        completedTables: state.nextIndex,
        totalTables: state.totalTables,
        log: `[WARN] Transient API issue for ${retryEntry ? retryEntry.country : 'current table'} (attempt ${retryAttempt}/${EQ_CONNECTED_SHEET_MAX_ATTEMPTS}): ${e.message}. Retrying...`
      };
    }

    clearConnectedSheetOperationState_(operationId);
    return {
      success: false,
      done: true,
      log: `[ERROR] Connected Sheets creation failed: ${e.message}`
    };
  }
}

function finalizeConnectedSheetSpreadsheet_(state) {
  if (!state || !state.spreadsheetId) {
    return;
  }

  const ss = SpreadsheetApp.openById(state.spreadsheetId);
  const sheets = ss.getSheets();
  if (!sheets || sheets.length <= 1) {
    return;
  }

  const toDelete = sheets.filter(function (sheet) {
    return sheet.getSheetId() === state.defaultSheetId || sheet.getName() === 'Sheet1';
  });

  toDelete.forEach(function (sheet) {
    try {
      ss.deleteSheet(sheet);
    } catch (e) {
      console.warn(`[EQ] Could not delete sheet "${sheet.getName()}": ${e.message}`);
    }
  });
}

function resolveConnectedSheetSourceByTable_(tableId) {
  let lastErr;

  for (let attempt = 1; attempt <= EQ_CONNECTED_SHEET_MAX_ATTEMPTS; attempt++) {
    try {
      BigQuery.Tables.get(EQ_PROJECT_ID, EQ_DATASET_ID, tableId);
      return { datasetId: EQ_DATASET_ID, tableId: tableId };
    } catch (e) {
      lastErr = e;
      if (!isTransientBigQueryError_(e) || attempt === EQ_CONNECTED_SHEET_MAX_ATTEMPTS) {
        break;
      }
      Utilities.sleep(1000 * attempt);
    }
  }

  throw new Error(
    `Connected Sheet source not found: ${EQ_PROJECT_ID}.${EQ_DATASET_ID}.${tableId}. ${lastErr.message}`
  );
}

function isTransientBigQueryError_(err) {
  const message = String((err && err.message) || err || '').toLowerCase();

  return (
    message.indexOf('empty response') !== -1 ||
    message.indexOf('internal error') !== -1 ||
    message.indexOf('backend error') !== -1 ||
    message.indexOf('rate limit') !== -1 ||
    message.indexOf('quota exceeded') !== -1 ||
    message.indexOf('timed out') !== -1 ||
    message.indexOf('deadline exceeded') !== -1 ||
    message.indexOf('http 500') !== -1 ||
    message.indexOf('http 503') !== -1
  );
}

function connectedSheetStateKey_(operationId) {
  return `EQ_CONNECTED_V3_${operationId}`;
}

function saveConnectedSheetOperationState_(operationId, state) {
  PropertiesService.getUserProperties().setProperty(
    connectedSheetStateKey_(operationId),
    JSON.stringify(state)
  );
}

function loadConnectedSheetOperationState_(operationId) {
  const raw = PropertiesService.getUserProperties().getProperty(connectedSheetStateKey_(operationId));
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function clearConnectedSheetOperationState_(operationId) {
  PropertiesService.getUserProperties().deleteProperty(connectedSheetStateKey_(operationId));
}