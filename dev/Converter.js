// ============================================================================
// CONFIGURATION
// ============================================================================
const UPLOADS_FOLDER_ID = '1ecRiWJON03Pd0qDNpRxNhTNDsYyw614Z';
const READY_FOLDER_ID = '16mMxz1DvsIEgKUk4mAXnamwxIQ50ddP5';
const LARGE_FILE_STAGE_THRESHOLD_BYTES = 10 * 1024 * 1024; // 10MB to safely trigger streaming
const SANITIZED_ERROR_CELL_VALUE = '';
const DATE_DAY_OFFSET = 1;
const ERROR_CELL_LOG_LIMIT = 200;
const ERROR_CELL_LOG_FALLBACK_DISPLAY = '(blank-display)';
const SHEETJS_MAX_FILE_BYTES = 100 * 1024 * 1024; 
const DRIVE_IMPORT_MAX_BYTES = 100 * 1024 * 1024; 

const SPREADSHEET_ERROR_MARKERS = [
  '#ERROR!', '#REF!', '#VALUE!', '#N/A', '#DIV/0!', '#NAME?', '#NUM!', 
  '#NULL!', '#WERT!', '#BEZUG!', '#NV', '#ZAHL!'
];

// Note: This array is largely obsolete now that all files use SheetJS by default,
// but it is kept here for backward compatibility or future specific logic.
const HEAVY_EXCEL_FILES = [
  "aktionsplan int", "wt stationär", "export pt", 
  "rwa", "ospl_artikelliste", "übersicht überschneiderartikel"
];

function getFolderConfigForConverter_() {
  if (typeof getPipelineFolderConfig === 'function') {
    try {
      return getPipelineFolderConfig();
    } catch (e) {
      console.warn('[CONVERTER] Falling back to hardcoded folder IDs: ' + e.message);
    }
  }

  return {
    uploads: { id: UPLOADS_FOLDER_ID, name: '01_Uploads' },
    ready: { id: READY_FOLDER_ID, name: '02_Ready' },
    archive: { id: '', name: '03_Archive' },
    output: { id: '', name: '04_Output' }
  };
}

// ============================================================================
// 1. MENU & UI TRIGGER
// ============================================================================
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu('Makro');
  let itemNumber = 1;

  // Helper function to dynamically add menu items only if the function exists
  function addIfAvailable(label, functionName) {
    const fn = globalThis[functionName];
    if (typeof fn === 'function') {
      menu.addItem(itemNumber + '. ' + label, functionName);
      itemNumber++;
    }
  }

  addIfAvailable('Convert Files (CSV)', 'openProgressUI');
  addIfAvailable('Import Ready Files to BigQuery', 'openBQProgressUI');
  addIfAvailable('Execute Transformations', 'openExecuteQueriesUI');

  menu.addToUi();
}

function onInstall(e) { onOpen(e); }

function openProgressUI() {
  const html = HtmlService.createHtmlOutputFromFile('ProgressUI')
    .setWidth(600).setHeight(500).setTitle('File Processing Terminal');
  SpreadsheetApp.getUi().showModalDialog(html, 'Automated File Converter');
}

// ============================================================================
// 2. FETCH FILES
// ============================================================================
function getPendingFiles() {
  const folderCfg = getFolderConfigForConverter_();
  const folder = DriveApp.getFolderById(folderCfg.uploads.id);
  const files = folder.getFiles();
  let fileList = [];
  while (files.hasNext()) {
    let f = files.next();
    fileList.push({ id: f.getId(), name: f.getName(), mimeType: f.getMimeType() });
  }
  return fileList; 
}

// ============================================================================
// 3. PROCESS A SINGLE FILE
// ============================================================================
function processSingleFile(fileObj) {
  const folderCfg = getFolderConfigForConverter_();
  const readyFolder = DriveApp.getFolderById(folderCfg.ready.id);
  const file = DriveApp.getFileById(fileObj.id);
  const lowerName = fileObj.name.toLowerCase();
  const fileSize = Number(file.getSize() || 0);
  
  let serverTrace = []; 
  function systemLog(msg) { console.log(msg); serverTrace.push(msg); }
  
  systemLog(`[SERVER] --- STARTING FILE: ${fileObj.name} ---`);
  systemLog(`[SERVER] File size: ${fileSize} bytes`);
  
  try {
    // If the file is already a CSV or Google Sheet, just move it to the ready folder
    if (lowerName.endsWith('.csv') || fileObj.mimeType === MimeType.CSV || fileObj.mimeType === 'text/csv' || fileObj.mimeType === MimeType.GOOGLE_SHEETS) {
      file.moveTo(readyFolder);
      return { success: true, log: `[SUCCESS] Moved file: ${fileObj.name}`, trace: serverTrace };
    }
    
    // Process Excel files (.xlsx, .xls, .xlsb)
    else if (lowerName.endsWith('.xlsx') || lowerName.endsWith('.xls') || lowerName.endsWith('.xlsb') || fileObj.mimeType.includes('spreadsheetml')) {
              
      let baseFileName = fileObj.name.replace(/\.(xlsx?|xlsb)$/i, '').trim();
      let csvName = `${baseFileName}.csv`;

      let isRwaFile = lowerName.includes('rwa');
      let isLargeFile = fileSize > LARGE_FILE_STAGE_THRESHOLD_BYTES;

      // SOLUTION APPLIED HERE: We force ALL non-large files to be processed by SheetJS.
      // This reads the cached values directly and prevents Google Sheets from recalculating
      // formulas like XLOOKUP, which usually results in #ERROR!.
      if (!isLargeFile) {
        try {
          let csvBlob = convertHeavyExcelWithSheetJS_(fileObj.id, csvName);
          readyFolder.createFile(csvBlob);
          file.setTrashed(true);
          return { success: true, log: `[SUCCESS] SheetJS Converted (Cached Values Kept): ${fileObj.name} -> ${csvName}`, trace: serverTrace };
        } catch (heavyError) {
          systemLog(`[SERVER] SheetJS fallback triggered: ${heavyError.message}`);
          // Fallback to native Google Drive conversion only if SheetJS fails
          return convertViaDrivePath_(file, fileObj, csvName, readyFolder, folderCfg.uploads.id, systemLog, serverTrace, { forceRwaDecimalStrings: isRwaFile });
        }
      }

      // --- STREAM TO SINGLE LARGE CSV ---
      // For files larger than the threshold, stream them to bypass Apps Script memory limits
      else if (isLargeFile) {
        systemLog(`[SERVER] Large file detected. Streaming to a single unified CSV file to bypass App Script limits...`);

        if (fileSize <= SHEETJS_MAX_FILE_BYTES) {
          try {
            let finalName = buildSingleLargeCsvFromWorksheetResumableWrapper_(fileObj.id, csvName, readyFolder.getId(), { forceRwaDecimalStrings: isRwaFile });
            file.setTrashed(true);
            return { success: true, log: `[SUCCESS] SheetJS Streamed: ${fileObj.name} -> ${finalName}`, trace: serverTrace };
          } catch (e) {
            systemLog(`[SERVER] SheetJS streaming failed: ${e.message}. Falling back to Drive conversion stream...`);
          }
        }

        // Abort if the file exceeds the absolute maximum limits
        if (fileSize > SHEETJS_MAX_FILE_BYTES && fileSize > DRIVE_IMPORT_MAX_BYTES) {
          return { success: false, log: `[ERROR] ${fileObj.name}: File is too large. Please convert to CSV externally.`, trace: serverTrace };
        }

        // Final fallback for large files using Google Drive streaming
        return convertViaDrivePath_(file, fileObj, csvName, readyFolder, folderCfg.uploads.id, systemLog, serverTrace, {
          forceRwaDecimalStrings: isRwaFile, resumable: true, skipBlobFallback: fileSize > LARGE_FILE_STAGE_THRESHOLD_BYTES
        });
      }
    }
    
    // Ignore unsupported formats
    else {
      return { success: true, log: `[IGNORED] Unsupported format: ${fileObj.name}`, trace: serverTrace };
    }
    
  } catch (error) {
    if (error.message.includes("Request Too Large")) {
      return { success: false, log: `[ERROR] ${fileObj.name}: Exceeded conversion limits.`, trace: serverTrace };
    }
    return { success: false, log: `[ERROR] ${fileObj.name}: ${error.message}`, trace: serverTrace };
  }
}

// ============================================================================
// 4. HELPER FUNCTIONS
// ============================================================================

function convertToGoogleSheet_(excelFile, folderId, options) {
  const settings = options || {};
  const metadata = { 
    name: excelFile.getName(), 
    mimeType: MimeType.GOOGLE_SHEETS, 
    parents: [folderId] 
  };

  let lastErr;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      return Drive.Files.copy(metadata, excelFile.getId(), { supportsAllDrives: true }).id;
    } catch (e) {
      lastErr = e;
      if (!/Internal Error|backendError|rate limit|timeout/i.test(String(e)) || attempt === 4) break;
      Utilities.sleep(1500 * attempt);
    }
  }

  if (settings.skipBlobFallback) {
    try {
      return convertLargeExcelViaResumableUpload_(excelFile.getId(), excelFile.getName(), folderId);
    } catch (e) { throw new Error(`Drive resumable upload failed: ${e.message}`); }
  }

  try {
    let inserted = Drive.Files.create(metadata, excelFile.getBlob(), { supportsAllDrives: true }); 
    return inserted.id;
  } catch (createErr) {
    // Acum returnăm eroarea reală care cauzează blocajul, nu eroarea de "fallback"
    throw new Error(`Drive conversion failed: ${createErr.message} (Copy attempt failed with: ${lastErr ? lastErr.message : 'N/A'})`); 
  }
}

// Full workflow for native Drive conversion, formatting, and CSV export
function convertViaDrivePath_(file, fileObj, csvName, readyFolder, folderId, systemLog, serverTrace, options) {
  const settings = options || {};
  let tempSheetId = convertToGoogleSheet_(file, folderId, { skipBlobFallback: !!settings.skipBlobFallback });
  let spreadsheet = SpreadsheetApp.openById(tempSheetId);
  let sheetToExport = spreadsheet.getSheets()[0];

  const sanitizeSummary = sanitizeSheetErrorCells_(sheetToExport, systemLog);
  const sanitizedErrorCells = sanitizeSummary.clearedCells;
  if (sanitizedErrorCells > 0) SpreadsheetApp.flush();

  applyMinimumDecimalFormatExceptFirstColumn_(sheetToExport);
  SpreadsheetApp.flush();

  if (settings.resumable) {
    systemLog(`[SERVER] Streaming directly into a single massive CSV file via Google Drive...`);
    let exportedName = exportSheetToSingleLargeCsvResumable_(sheetToExport, csvName, readyFolder.getId(), { forceRwaDecimalStrings: !!settings.forceRwaDecimalStrings });
    
    DriveApp.getFileById(tempSheetId).setTrashed(true);
    file.setTrashed(true);
    return { success: true, log: `[SUCCESS] Streamed (Single File): ${fileObj.name} -> ${exportedName}`, trace: serverTrace };
  }

  let csvBlob = buildCsvBlobFromSheet_(sheetToExport, csvName, { forceRwaDecimalStrings: !!settings.forceRwaDecimalStrings });
  readyFolder.createFile(csvBlob);

  DriveApp.getFileById(tempSheetId).setTrashed(true);
  file.setTrashed(true);
  return { success: true, log: `[SUCCESS] Converted: ${fileObj.name} -> ${csvName}`, trace: serverTrace };
}

// ----------------------------------------------------------------------------
// RESUMABLE UPLOAD EXPORTERS
// ----------------------------------------------------------------------------

// Streams a Google Sheet directly into a large CSV file using resumable uploads
function exportSheetToSingleLargeCsvResumable_(sheet, fileName, folderId, options) {
  const settings = options || {};
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow === 0 || lastColumn === 0) {
    DriveApp.getFolderById(folderId).createFile(fileName, '', MimeType.CSV);
    return fileName;
  }

  const token = ScriptApp.getOAuthToken();
  const metadata = JSON.stringify({ name: fileName, mimeType: MimeType.CSV, parents: [folderId] });
  const initResp = UrlFetchApp.fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true', {
    method: 'post', contentType: 'application/json; charset=UTF-8', headers: { 'Authorization': 'Bearer ' + token }, payload: metadata, muteHttpExceptions: true
  });
  const uploadUrl = initResp.getHeaders()['Location'] || initResp.getHeaders()['location'];

  const timezone = resolveSheetTimeZone_(sheet);
  let uploadBuffer = [];
  let totalUploaded = 0;
  const CHUNK_MULTIPLE = 262144; 
  const UPLOAD_CHUNK_SIZE = CHUNK_MULTIPLE * 8; 

  // Pushes buffered data to the resumable upload endpoint
  function pushToUpload(isFinal) {
    while (uploadBuffer.length >= UPLOAD_CHUNK_SIZE || (isFinal && uploadBuffer.length >= 0)) {
      let bytesToUpload;
      if (isFinal && uploadBuffer.length < UPLOAD_CHUNK_SIZE) {
        bytesToUpload = uploadBuffer;
        uploadBuffer = [];
      } else {
        let size = Math.floor(uploadBuffer.length / CHUNK_MULTIPLE) * CHUNK_MULTIPLE;
        if (size > UPLOAD_CHUNK_SIZE) size = UPLOAD_CHUNK_SIZE;
        if (size === 0 && !isFinal) break; 
        bytesToUpload = uploadBuffer.slice(0, size);
        uploadBuffer = uploadBuffer.slice(size);
      }

      let rangeHeader;
      if (bytesToUpload.length === 0 && isFinal) {
        if (totalUploaded === 0) break;
        rangeHeader = `bytes */${totalUploaded}`;
      } else {
        let start = totalUploaded;
        let end = totalUploaded + bytesToUpload.length - 1;
        let total = isFinal ? totalUploaded + bytesToUpload.length : '*';
        rangeHeader = `bytes ${start}-${end}/${total}`;
      }
      
      let fetchOptions = { method: 'put', headers: { 'Content-Range': rangeHeader }, muteHttpExceptions: true };
      if (bytesToUpload.length > 0) fetchOptions.payload = bytesToUpload;

      let resp = UrlFetchApp.fetch(uploadUrl, fetchOptions);
      let code = resp.getResponseCode();
      
      if (code === 200 || code === 201) break; 
      if (code !== 308) throw new Error(`Stream Error: HTTP ${code}`);
      
      totalUploaded += bytesToUpload.length;
      if (isFinal && uploadBuffer.length === 0) break;
    }
  }

  // Process headers
  const headerRange = sheet.getRange(1, 1, 1, lastColumn);
  const headerValues = headerRange.getValues()[0];
  const headerFormats = headerRange.getNumberFormats()[0];
  let headerCsv = headerValues.map((v, i) => escapeCsvValue_(normalizeSheetValueForCsv_(v, headerFormats[i], i > 0, timezone, settings))).join(',') + '\n';
    
  let hBytes = Utilities.newBlob(headerCsv).getBytes();
  for(let i=0; i<hBytes.length; i++) uploadBuffer.push(hBytes[i]);

  // Process rows in batches to avoid memory limits
  const ROW_BATCH = 5000; 
  for (let startRow = 2; startRow <= lastRow; startRow += ROW_BATCH) {
    let rowsInBatch = Math.min(ROW_BATCH, lastRow - startRow + 1);
    let dataRange = sheet.getRange(startRow, 1, rowsInBatch, lastColumn);
    let values = dataRange.getValues();
    let formats = dataRange.getNumberFormats();
    
    let batchLines = [];
    for (let r = 0; r < values.length; r++) {
      batchLines.push(values[r].map((v, c) => escapeCsvValue_(normalizeSheetValueForCsv_(v, formats[r][c], c > 0, timezone, settings))).join(','));
    }
    
    let batchCsvStr = batchLines.join('\n') + '\n';
    let batchBytes = Utilities.newBlob(batchCsvStr).getBytes();
    for(let i=0; i<batchBytes.length; i++) uploadBuffer.push(batchBytes[i]);
    
    pushToUpload(false);
  }
  
  pushToUpload(true);
  return fileName;
}

// Wrapper to initialize SheetJS and stream a large Excel file into CSV
function buildSingleLargeCsvFromWorksheetResumableWrapper_(fileId, csvFileName, folderId, options) {
  const sheetJSUrl = "https://cdn.sheetjs.com/xlsx-0.19.3/package/dist/xlsx.full.min.js";
  const scriptText = UrlFetchApp.fetch(sheetJSUrl).getContentText();
  eval(scriptText);
  globalThis.XLSX = XLSX;

  const u8 = downloadLargeFileBytes_(fileId);
  
  // Explicitly ignore formulas and rely purely on cached display values to prevent #ERROR!
  const workbook = XLSX.read(u8, { 
    type: 'array', 
    cellDates: true,
    cellFormula: false,
    cellHTML: false
  });
  
  const firstSheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[firstSheetName];

  return buildSingleLargeCsvFromWorksheetResumable_(worksheet, csvFileName, folderId, options);
}

// Processes the SheetJS worksheet directly into a resumable upload chunk stream
function buildSingleLargeCsvFromWorksheetResumable_(worksheet, fileName, folderId, options) {
  const settings = options || {};
  const ref = worksheet['!ref'];
  if (!ref) {
    DriveApp.getFolderById(folderId).createFile(fileName, '', MimeType.CSV);
    return fileName;
  }

  const token = ScriptApp.getOAuthToken();
  const metadata = JSON.stringify({ name: fileName, mimeType: MimeType.CSV, parents: [folderId] });
  const initResp = UrlFetchApp.fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true', {
    method: 'post', contentType: 'application/json; charset=UTF-8', headers: { 'Authorization': 'Bearer ' + token }, payload: metadata, muteHttpExceptions: true
  });
  const uploadUrl = initResp.getHeaders()['Location'] || initResp.getHeaders()['location'];
  const range = XLSX.utils.decode_range(ref);
  
  let uploadBuffer = [];
  let totalUploaded = 0;
  const CHUNK_MULTIPLE = 262144; 
  const UPLOAD_CHUNK_SIZE = CHUNK_MULTIPLE * 8;

  function pushToUpload(isFinal) {
    while (uploadBuffer.length >= UPLOAD_CHUNK_SIZE || (isFinal && uploadBuffer.length >= 0)) {
      let bytesToUpload;
      if (isFinal && uploadBuffer.length < UPLOAD_CHUNK_SIZE) {
        bytesToUpload = uploadBuffer;
        uploadBuffer = [];
      } else {
        let size = Math.floor(uploadBuffer.length / CHUNK_MULTIPLE) * CHUNK_MULTIPLE;
        if (size > UPLOAD_CHUNK_SIZE) size = UPLOAD_CHUNK_SIZE;
        if (size === 0 && !isFinal) break;
        bytesToUpload = uploadBuffer.slice(0, size);
        uploadBuffer = uploadBuffer.slice(size);
      }

      let rangeHeader;
      if (bytesToUpload.length === 0 && isFinal) {
        if (totalUploaded === 0) break;
        rangeHeader = `bytes */${totalUploaded}`;
      } else {
        let start = totalUploaded;
        let end = totalUploaded + bytesToUpload.length - 1;
        let total = isFinal ? totalUploaded + bytesToUpload.length : '*';
        rangeHeader = `bytes ${start}-${end}/${total}`;
      }
      
      let fetchOptions = { method: 'put', headers: { 'Content-Range': rangeHeader }, muteHttpExceptions: true };
      if (bytesToUpload.length > 0) fetchOptions.payload = bytesToUpload;
      
      let resp = UrlFetchApp.fetch(uploadUrl, fetchOptions);
      let code = resp.getResponseCode();
      if (code === 200 || code === 201) break;
      if (code !== 308) throw new Error(`Stream Error: HTTP ${code}`);
      
      totalUploaded += bytesToUpload.length;
      if (isFinal && uploadBuffer.length === 0) break;
    }
  }

  const headerValues = [];
  for (let c = range.s.c; c <= range.e.c; c++) {
    const addr = XLSX.utils.encode_cell({ r: range.s.r, c: c });
    headerValues.push(escapeCsvValue_(sheetJsRawCellValue_(worksheet[addr], c > range.s.c, settings)));
  }
  let hBytes = Utilities.newBlob(headerValues.join(',') + '\n').getBytes();
  for(let i=0; i<hBytes.length; i++) uploadBuffer.push(hBytes[i]);

  const dataStartRow = range.s.r + 1;
  const ROW_BATCH = 10000;
  for (let startRow = dataStartRow; startRow <= range.e.r; startRow += ROW_BATCH) {
    let endRow = Math.min(startRow + ROW_BATCH - 1, range.e.r);
    let batchLines = [];
    for (let r = startRow; r <= endRow; r++) {
      let rowVals = [];
      for (let c = range.s.c; c <= range.e.c; c++) {
        const addr = XLSX.utils.encode_cell({ r: r, c: c });
        rowVals.push(escapeCsvValue_(sheetJsRawCellValue_(worksheet[addr], c > range.s.c, settings)));
      }
      batchLines.push(rowVals.join(','));
    }
    
    let batchBytes = Utilities.newBlob(batchLines.join('\n') + '\n').getBytes();
    for(let i=0; i<batchBytes.length; i++) uploadBuffer.push(batchBytes[i]);
    pushToUpload(false);
  }

  pushToUpload(true);
  return fileName;
}

// ----------------------------------------------------------------------------
// GENERAL HELPER FUNCTIONS
// ----------------------------------------------------------------------------

// Converts an Excel file directly to CSV using the SheetJS library in-memory
function convertHeavyExcelWithSheetJS_(fileId, csvFileName) {
  const sheetJSUrl = "https://cdn.sheetjs.com/xlsx-0.19.3/package/dist/xlsx.full.min.js";
  const scriptText = UrlFetchApp.fetch(sheetJSUrl).getContentText();
  eval(scriptText);
  globalThis.XLSX = XLSX;
  
  const file = DriveApp.getFileById(fileId);
  const u8 = new Uint8Array(file.getBlob().getBytes());
  
  // Explicitly ignore formulas and rely purely on cached display values
  const workbook = XLSX.read(u8, {
    type: 'array', 
    cellDates: true,
    cellFormula: false,
    cellHTML: false
  });
  
  const worksheet = workbook.Sheets[workbook.SheetNames[0]];
  
  const csvString = worksheetToCsvPreservingRawValues_(worksheet, { forceRwaDecimalStrings: csvFileName.toLowerCase().includes('rwa') }); 
  return Utilities.newBlob(csvString, MimeType.CSV, csvFileName);
}

// Downloads large files in chunks to avoid App Script memory exhaustion limits
function downloadLargeFileBytes_(fileId) {
  const file = DriveApp.getFileById(fileId);
  const fileSize = Number(file.getSize() || 0);
  
  if (fileSize <= 50 * 1024 * 1024) {
    return new Uint8Array(file.getBlob().getBytes());
  }

  const RANGE_CHUNK = 25 * 1024 * 1024;
  const url = 'https://www.googleapis.com/drive/v3/files/' + fileId + '?alt=media';
  const token = ScriptApp.getOAuthToken();
  const result = new Uint8Array(fileSize);
  let offset = 0;

  while (offset < fileSize) {
    const end = Math.min(offset + RANGE_CHUNK - 1, fileSize - 1);
    const resp = UrlFetchApp.fetch(url, { 
      headers: { 
        'Authorization': 'Bearer ' + token, 
        'Range': 'bytes=' + offset + '-' + end,
        'Accept-Encoding': 'identity' 
      }, 
      muteHttpExceptions: true 
    });
    
    const chunk = resp.getContent();
    for (let i = 0; i < chunk.length; i++) result[offset + i] = chunk[i];
    offset += chunk.length;
  }
  return result;
}

// Uploads a large file to Google Drive directly via Resumable API chunking
function convertLargeExcelViaResumableUpload_(fileId, fileName, folderId) {
  const token = ScriptApp.getOAuthToken();
  const fileSize = Number(DriveApp.getFileById(fileId).getSize() || 0);
  const metadataPayload = JSON.stringify({ name: fileName, mimeType: 'application/vnd.google-apps.spreadsheet', parents: [folderId] });

  const initResp = UrlFetchApp.fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true', {
      method: 'post', contentType: 'application/json; charset=UTF-8', headers: { 'Authorization': 'Bearer ' + token, 'X-Upload-Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'X-Upload-Content-Length': String(fileSize) },
      payload: metadataPayload, muteHttpExceptions: true
  });

  const uploadUrl = initResp.getHeaders()['Location'] || initResp.getHeaders()['location'];
  const CHUNK = 5 * 1024 * 1024;
  const downloadUrl = 'https://www.googleapis.com/drive/v3/files/' + fileId + '?alt=media';
  let offset = 0;

  while (offset < fileSize) {
    const end = Math.min(offset + CHUNK - 1, fileSize - 1);
    const expectedLen = end - offset + 1;

    var dlResp = UrlFetchApp.fetch(downloadUrl, { headers: { 'Authorization': 'Bearer ' + token, 'Range': 'bytes=' + offset + '-' + end }, muteHttpExceptions: true });
    var dlBlob = dlResp.getBlob();

    const contentRange = 'bytes ' + offset + '-' + (offset + expectedLen - 1) + '/' + fileSize;
    var ulResp = UrlFetchApp.fetch(uploadUrl, { method: 'put', contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers: { 'Content-Range': contentRange }, payload: dlBlob, muteHttpExceptions: true });
    
    const ulCode = ulResp.getResponseCode();
    if (ulCode === 200 || ulCode === 201) return JSON.parse(ulResp.getContentText()).id;
    if (ulCode === 308) offset += expectedLen;
    else throw new Error(`HTTP ${ulCode} - ${ulResp.getContentText()}`);
  }
}

// Iterates through a SheetJS worksheet object to assemble a CSV string
function worksheetToCsvPreservingRawValues_(worksheet, options) {
  const settings = options || {};
  const ref = worksheet['!ref'];
  if (!ref) return '';
  const range = XLSX.utils.decode_range(ref);
  const rows = [];
  for (let rowIndex = range.s.r; rowIndex <= range.e.r; rowIndex++) {
    const values = [];
    for (let colIndex = range.s.c; colIndex <= range.e.c; colIndex++) {
      const address = XLSX.utils.encode_cell({ r: rowIndex, c: colIndex });
      values.push(escapeCsvValue_(sheetJsRawCellValue_(worksheet[address], colIndex > range.s.c, settings)));
    }
    rows.push(values.join(','));
  }
  return rows.join('\n');
}

// Extracts data from a native Google Sheet object to build a CSV Blob
function buildCsvBlobFromSheet_(sheet, fileName, options) {
  const settings = options || {};
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();
  if (lastRow === 0 || lastColumn === 0) return Utilities.newBlob('', MimeType.CSV, fileName);

  const range = sheet.getRange(1, 1, lastRow, lastColumn);
  const values = range.getValues();
  const formats = range.getNumberFormats();
  const timezone = resolveSheetTimeZone_(sheet);
  const csvString = values.map((row, rowIdx) => row.map((value, colIdx) => escapeCsvValue_(normalizeSheetValueForCsv_(value, formats[rowIdx][colIdx], colIdx > 0, timezone, settings))).join(',')).join('\n');
  return Utilities.newBlob(csvString, MimeType.CSV, fileName);
}

// ----------------------------------------------------------------------------
// We no longer clear error cells in order to preserve the displayed text 
// ----------------------------------------------------------------------------
function sanitizeSheetErrorCells_(sheet, systemLog) {
  return { clearedCells: 0 };
}

// Checks if a cell contains standard Excel/Sheets error tags
function detectSpreadsheetErrorMarker_(displayValue) {
  const token = String(displayValue == null ? '' : displayValue).trim().toUpperCase();
  if (!token) return '';
  if (SPREADSHEET_ERROR_MARKERS.indexOf(token) !== -1) return token;
  return /^#\S+$/.test(token) ? token : '';
}

// ----------------------------------------------------------------------------
// Normalizes and extracts the exact string for CSV export 
// (even if it's the result of an XLOOKUP)
// ----------------------------------------------------------------------------
function normalizeSheetValueForCsv_(value, numberFormat, enforceMinimumDecimals, timezone, options) {
  const settings = options || {};
  if (value == null || value === '') return '';
  
  if (value instanceof Date) return formatDateForCsv_(value, timezone);
  
  if (typeof value === 'number') {
    if (isDateLikeNumberFormat_(numberFormat)) return formatGoogleSheetsSerialDate_(value);
    if (settings.forceRwaDecimalStrings) return normalizeBigNumericString_(value);
    return enforceMinimumDecimals ? formatNumberAtLeast5Decimals_(value) : String(value);
  }
  
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
  
  return String(value);
}

// ----------------------------------------------------------------------------
// Extracts `cell.w` from SheetJS object 
// (The formatted/displayed value visible on screen in Excel)
// ----------------------------------------------------------------------------
function sheetJsRawCellValue_(cell, enforceMinimumDecimals, options) {
  const settings = options || {};
  if (!cell) return '';
  
  if (cell.t === 'd' && cell.v instanceof Date) {
    const shiftedDate = shiftYmdByOffset_(cell.v.getUTCFullYear(), cell.v.getUTCMonth() + 1, cell.v.getUTCDate(), DATE_DAY_OFFSET);
    return formatDatePartsAsMdy_(shiftedDate.year, shiftedDate.month, shiftedDate.day);
  }
  
  if (cell.t === 'n') {
    if (settings.forceRwaDecimalStrings) return normalizeBigNumericString_(cell.v);
    return enforceMinimumDecimals ? formatNumberAtLeast5Decimals_(cell.v) : String(cell.v);
  }
  
  if (cell.t === 'b') return cell.v ? 'TRUE' : 'FALSE';
  
  // EXTRACT DISPLAY VALUE:
  // cell.w contains the text exactly as visible in Excel.
  let displayVal = cell.w !== undefined ? cell.w : cell.v;
  let finalString = String(displayVal == null ? '' : displayVal);
  
  return finalString;
}

// Formats a Date object to MM/DD/YYYY format for CSV
function formatDateForCsv_(dateObj, timezone) {
  const tz = timezone || Session.getScriptTimeZone() || 'UTC';
  const year = parseInt(Utilities.formatDate(dateObj, tz, 'yyyy'), 10);
  const month = parseInt(Utilities.formatDate(dateObj, tz, 'MM'), 10);
  const day = parseInt(Utilities.formatDate(dateObj, tz, 'dd'), 10);
  const shiftedDate = shiftYmdByOffset_(year, month, day, DATE_DAY_OFFSET);
  return formatDatePartsAsMdy_(shiftedDate.year, shiftedDate.month, shiftedDate.day);
}

// Safely attempts to retrieve the Spreadsheet timezone, defaulting to script/UTC
function resolveSheetTimeZone_(sheet) {
  try { return sheet.getParent().getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'UTC'; } 
  catch (e) { return Session.getScriptTimeZone() || 'UTC'; }
}

// Uses regex to determine if a generic Google Sheets format string relates to dates
function isDateLikeNumberFormat_(numberFormat) {
  const fmt = String(numberFormat == null ? '' : numberFormat).toLowerCase();
  return /(^|[^a-z])(d|dd|ddd|dddd|m|mm|mmm|mmmm|yy|yyyy)([^a-z]|$)/i.test(fmt) && !/(^|[^a-z])(h|hh|s|ss)([^a-z]|$)/i.test(fmt) || /(d|m|y)/i.test(fmt);
}

// Converts an integer serial number back into a readable Date string
function formatGoogleSheetsSerialDate_(serial) {
  if (typeof serial !== 'number' || !isFinite(serial)) return '';
  const dt = new Date(Math.round((serial - 25569) * 86400000));
  const shiftedDate = shiftYmdByOffset_(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate(), DATE_DAY_OFFSET);
  return formatDatePartsAsMdy_(shiftedDate.year, shiftedDate.month, shiftedDate.day);
}

// Offsets dates by a specific number of days if needed due to system discrepancies
function shiftYmdByOffset_(year, month, day, offsetDays) {
  const shifted = new Date(Date.UTC(year, month - 1, day + offsetDays));
  return { year: shifted.getUTCFullYear(), month: shifted.getUTCMonth() + 1, day: shifted.getUTCDate() };
}

// Removes problematic commas or decimals from large numerical identifiers
function normalizeBigNumericString_(value) {
  let str = String(value == null ? '' : value).trim().replace(/\s+/g, '');
  if (/[eE]/.test(str)) str = formatNumberAtLeast5Decimals_(Number(str));
  if (str.indexOf(',') !== -1 && str.indexOf('.') !== -1) str = str.replace(/,/g, '');
  else if (str.indexOf(',') !== -1) str = str.replace(/\./g, '').replace(/,/g, '.');
  return str;
}

// Simple template literal injection for consistent date formatting
function formatDatePartsAsMdy_(year, month, day) {
  return `${String(month).padStart(2, '0')}/${String(day).padStart(2, '0')}/${String(year).padStart(4, '0')}`;
}

// Applies a massive 15-decimal place generic format to all rows except column 1
function applyMinimumDecimalFormatExceptFirstColumn_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();
  if (lastRow > 0 && lastColumn > 1) sheet.getRange(1, 2, lastRow, lastColumn - 1).setNumberFormat('0.00000###############');
}

// Ensures specific numerical variables maintain at least 5 decimal positions
function formatNumberAtLeast5Decimals_(num) {
  let str = String(num);
  if (/[eE]/.test(str)) {
    const [coeff, expPart] = str.toLowerCase().split('e');
    const exp = parseInt(expPart, 10);
    const [intStr, decStr = ''] = coeff.split('.');
    const isNeg = intStr.startsWith('-');
    const allDigits = intStr.replace('-', '') + decStr;
    const newDotPos = intStr.replace('-', '').length + exp; 
    if (newDotPos <= 0) str = (isNeg ? '-' : '') + '0.' + '0'.repeat(-newDotPos) + allDigits;
    else if (newDotPos >= allDigits.length) str = (isNeg ? '-' : '') + allDigits + '0'.repeat(newDotPos - allDigits.length);
    else str = (isNeg ? '-' : '') + allDigits.slice(0, newDotPos) + '.' + allDigits.slice(newDotPos);
  }
  const dotIdx = str.indexOf('.');
  if (dotIdx === -1) return str + '.00000';
  const currentDecimals = str.length - dotIdx - 1;
  return currentDecimals < 5 ? str + '0'.repeat(5 - currentDecimals) : str;
}

// Wraps CSV text nodes in quotation marks if they contain commas or line breaks
function escapeCsvValue_(value) {
  // Replace internal newlines with a space to prevent BigQuery row/column shifting.
  // Cells with Alt+Enter in Excel would otherwise split into multiple CSV rows
  // and shift all subsequent columns for that record.
  const text = String(value == null ? '' : value).replace(/[\r\n]+/g, ' ').trim();
  return /[",]/.test(text) ? '"' + text.replace(/"/g, '""') + '"' : text;
}