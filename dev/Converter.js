// ============================================================================
// CONFIGURATION
// ============================================================================
const UPLOADS_FOLDER_ID = '1ecRiWJON03Pd0qDNpRxNhTNDsYyw614Z';
const READY_FOLDER_ID = '16mMxz1DvsIEgKUk4mAXnamwxIQ50ddP5';

// NEW: Files that crash Google's converter and need in-memory SheetJS parsing
const HEAVY_EXCEL_FILES = [
  "aktionsplan int", 
  "wt stationär", 
  "export pt_de", 
  "rwa",
  "ospl_artikelliste",
  "übersicht überschneiderartikel"
];

// ============================================================================
// 1. MENU & UI TRIGGER
// ============================================================================
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu('Lagerliste');
  let itemNumber = 1;

  function addIfAvailable(label, functionName) {
    const fn = globalThis[functionName];
    if (typeof fn === 'function') {
      menu.addItem(itemNumber + '. ' + label, functionName);
      itemNumber++;
    } else {
      console.log(`[MENU] Skipped missing action: ${functionName}`);
    }
  }

  addIfAvailable('Convert Files (CSV)', 'openProgressUI');
  addIfAvailable('Import Ready Files to BigQuery', 'openBQProgressUI');
  addIfAvailable('Run SQL Transformations (PT + RWA)', 'openTransformUI');

  menu.addToUi();
}

function onInstall(e) {
  onOpen(e);
}

function openProgressUI() {
  const html = HtmlService.createHtmlOutputFromFile('ProgressUI')
    .setWidth(600)
    .setHeight(500)
    .setTitle('File Processing Terminal');
  SpreadsheetApp.getUi().showModalDialog(html, 'Automated File Converter');
}

// ============================================================================
// 2. FETCH FILES
// ============================================================================
function getPendingFiles() {
  console.log("[INIT] Scanning 01_Uploads for pending files...");
  const folder = DriveApp.getFolderById(UPLOADS_FOLDER_ID);
  const files = folder.getFiles();
  let fileList = [];
  
  while (files.hasNext()) {
    let f = files.next();
    fileList.push({ id: f.getId(), name: f.getName(), mimeType: f.getMimeType() });
  }
  
  console.log(`[INIT] Found ${fileList.length} files. Passing queue to UI.`);
  return fileList; 
}

// ============================================================================
// 3. PROCESS A SINGLE FILE
// ============================================================================
function processSingleFile(fileObj) {
  const readyFolder = DriveApp.getFolderById(READY_FOLDER_ID);
  const file = DriveApp.getFileById(fileObj.id);
  const lowerName = fileObj.name.toLowerCase();
  
  let logMessage = "";
  let serverTrace = []; 
  
  // Dual-logger: Writes to Apps Script Executions AND the UI array
  function systemLog(msg) {
    console.log(msg);
    serverTrace.push(msg);
  }
  
  systemLog(`[SERVER] --- STARTING FILE: ${fileObj.name} ---`);
  systemLog(`[SERVER] Received ID: ${fileObj.id} | Type: ${fileObj.mimeType}`);
  
  try {
    // --- SCENARIO A: CSV OR GOOGLE SHEET FILE ---
    // Added MimeType.GOOGLE_SHEETS here so it moves directly without conversion
    if (lowerName.endsWith('.csv') || fileObj.mimeType === MimeType.CSV || fileObj.mimeType === 'text/csv' || fileObj.mimeType === MimeType.GOOGLE_SHEETS) {
      systemLog(`[SERVER] Valid CSV or Native Google Sheet detected. Moving directly to 02_Ready.`);
      file.moveTo(readyFolder);
      return { success: true, log: `[SUCCESS] Moved file: ${fileObj.name}`, trace: serverTrace };
    }
    
    // --- SCENARIO B: EXCEL / XLSB FILE ---
    else if (lowerName.endsWith('.xlsx') || lowerName.endsWith('.xls') || lowerName.endsWith('.xlsb') ||
             fileObj.mimeType === MimeType.MICROSOFT_EXCEL || 
             fileObj.mimeType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
             fileObj.mimeType === 'application/vnd.ms-excel.sheet.binary.macroEnabled.12') {
             
      let baseFileName = fileObj.name.replace(/\.(xlsx?|xlsb)$/i, '').trim();
      let csvName = `${baseFileName}.csv`;

      // Check if this file requires our SheetJS bypass engine
      let isHeavyFile = HEAVY_EXCEL_FILES.some(keyword => lowerName.includes(keyword));
      let isXlsb = lowerName.endsWith('.xlsb') || fileObj.mimeType === 'application/vnd.ms-excel.sheet.binary.macroEnabled.12';

      // --- PATH 1: SHEETJS IN-MEMORY PARSING (Heavy files & .xlsb) ---
      if (isHeavyFile || isXlsb) {
        systemLog(`[SERVER] Detected heavy/binary file. Bypassing Drive API.`);
        systemLog(`[SERVER] Booting up SheetJS In-Memory Engine...`);

        try {
          let csvBlob = convertHeavyExcelWithSheetJS_(fileObj.id, csvName);

          systemLog(`[SERVER] In-Memory parsing successful. Writing to 02_Ready folder...`);
          readyFolder.createFile(csvBlob);

          systemLog(`[SERVER] Trashing original heavy file...`);
          file.setTrashed(true);

          return { success: true, log: `[SUCCESS] SheetJS Converted: ${fileObj.name} -> ${csvName}`, trace: serverTrace };
        } catch (heavyError) {
          const msg = String(heavyError && heavyError.message || heavyError);
          if (/exceeds the maximum file size|maximum file size|Request Too Large/i.test(msg)) {
            systemLog(`[SERVER] SheetJS path failed due to file size. Falling back to Drive conversion path...`);
            return convertViaDrivePath_(file, fileObj, csvName, readyFolder, UPLOADS_FOLDER_ID, systemLog, serverTrace);
          }
          throw heavyError;
        }
      } 
      
      // --- PATH 2: STANDARD GOOGLE DRIVE API CONVERSION ---
      else {
        return convertViaDrivePath_(file, fileObj, csvName, readyFolder, UPLOADS_FOLDER_ID, systemLog, serverTrace);
      }
    }
    
    // --- SCENARIO C: IGNORED FILE ---
    else {
      systemLog(`[SERVER] File type unsupported. Ignoring.`);
      return { success: true, log: `[IGNORED] Unsupported format: ${fileObj.name}`, trace: serverTrace };
    }
    
  } catch (error) {
    systemLog(`[SERVER] SYSTEM CRASH: ${error.message}`);
    if (error.message.includes("Request Too Large")) {
      return { success: false, log: `[ERROR] ${fileObj.name}: File too massive for cloud conversion. MUST be uploaded as CSV.`, trace: serverTrace };
    }
    return { success: false, log: `[ERROR] ${fileObj.name}: ${error.message}`, trace: serverTrace };
  }
}

// ============================================================================
// 4. HELPER FUNCTIONS
// ============================================================================
function convertToGoogleSheet_(excelFile, folderId) {
  const metadata = { name: excelFile.getName(), mimeType: MimeType.GOOGLE_SHEETS, parents: [folderId] };

  // Path A: Copy+convert with retries (fastest, but can return transient "Internal Error")
  let lastErr;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const newFile = Drive.Files.copy(metadata, excelFile.getId(), { supportsAllDrives: true });
      return newFile.id;
    } catch (e) {
      lastErr = e;
      const msg = String(e && e.message || e);
      const transient = /Internal Error|backendError|rate limit|timeout/i.test(msg);
      if (!transient || attempt === 4) break;
      Utilities.sleep(1500 * attempt);
    }
  }

  // Path B: Fallback to insert+convert using media blob
  // This bypasses occasional copy() backend failures for some files.
  try {
    const inserted = Drive.Files.insert(
      metadata,
      excelFile.getBlob(),
      { convert: true, supportsAllDrives: true }
    );
    return inserted.id;
  } catch (fallbackErr) {
    const primaryMsg = String(lastErr && lastErr.message || lastErr || 'unknown');
    const fallbackMsg = String(fallbackErr && fallbackErr.message || fallbackErr || 'unknown');
    throw new Error(`Drive conversion failed. copy() error: ${primaryMsg} | insert(convert) error: ${fallbackMsg}`);
  }
}

function exportSheetAsCsvBlob_(spreadsheetId, sheetId, fileName) {
  let url = `https://docs.google.com/spreadsheets/d/${spreadsheetId}/export?format=csv&gid=${sheetId}`;
  
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      let response = UrlFetchApp.fetch(url, {
        headers: { 'Authorization': 'Bearer ' + ScriptApp.getOAuthToken() },
        muteHttpExceptions: true
      });
      if (response.getResponseCode() === 200) return response.getBlob().setName(fileName);
    } catch (e) { if (attempt === 3) throw e; }
    Utilities.sleep(2000); 
  }
  throw new Error("Failed to export tab after 3 attempts.");
}

/**
 * Bypasses Google Drive API limits by parsing complex or binary files 
 * completely in-memory using the open-source SheetJS library.
 */
function convertHeavyExcelWithSheetJS_(fileId, csvFileName) {
  // 1. Fetch SheetJS Library via CDN and load it into Apps Script Memory
  const sheetJSUrl = "https://cdn.sheetjs.com/xlsx-0.19.3/package/dist/xlsx.full.min.js";
  const scriptText = UrlFetchApp.fetch(sheetJSUrl).getContentText();
  eval(scriptText); // Executes the library to make 'XLSX' available in local scope
  globalThis.XLSX = XLSX; // Promote to global scope so helper functions can access it
  
  // 2. Download the heavy file into memory as a byte array
  const file = DriveApp.getFileById(fileId);
  const bytes = file.getBlob().getBytes();
  
  // 3. Convert Apps Script signed bytes to Unsigned Integer Array for SheetJS
  const u8 = new Uint8Array(bytes);
  
  // 4. Parse the workbook (cellDates: true ensures dates don't turn into integer serials)
  const workbook = XLSX.read(u8, {type: 'array', cellDates: true});
  
  // 5. Target the first sheet
  const firstSheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[firstSheetName];
  
  // 6. Convert the sheet to CSV using raw cell values (cell.v) instead of 
  // the display-formatted values (cell.w) to preserve full numeric precision.
  const csvString = worksheetToCsvPreservingRawValues_(worksheet); 
  
  // 7. Package it as a Blob ready to be saved
  return Utilities.newBlob(csvString, MimeType.CSV, csvFileName);
}

function convertViaDrivePath_(file, fileObj, csvName, readyFolder, folderId, systemLog, serverTrace) {
  systemLog(`[SERVER] Instructing Google Drive to convert Excel file...`);
  let tempSheetId = convertToGoogleSheet_(file, folderId);
  systemLog(`[SERVER] Drive API success. Temp Sheet ID: ${tempSheetId}`);

  systemLog(`[SERVER] Opening sheet to extract data...`);
  let spreadsheet = SpreadsheetApp.openById(tempSheetId);
  let sheetToExport = spreadsheet.getSheets()[0];

  systemLog(`[SERVER] Targeted first tab: [${sheetToExport.getName()}]`);

  // --- QUALITY ASSURANCE: CHECK FOR BROKEN FORMULAS ---
  systemLog(`[SERVER] Running QA scan for broken formulas...`);
  let hasError = sheetToExport.createTextFinder("#ERROR!").findNext();
  let hasRef = sheetToExport.createTextFinder("#REF!").findNext();

  if (hasError || hasRef) {
    systemLog(`[SERVER] CRITICAL: Found #ERROR! or #REF! inside file.`);
    DriveApp.getFileById(tempSheetId).setTrashed(true);
    file.setTrashed(true);
    systemLog(`[SERVER] Trashed temporary sheet and rejected original file.`);

    return {
      success: false,
      log: `[REJECTED] ${fileObj.name}: Contains broken formulas (#ERROR!). Deleted. Please save as CSV locally and re-upload.`,
      trace: serverTrace
    };
  }
  systemLog(`[SERVER] QA Passed: No broken formulas detected.`);

  systemLog(`[SERVER] Applying minimum 5-decimal formatting to all columns except the first...`);
  applyMinimumDecimalFormatExceptFirstColumn_(sheetToExport);
  SpreadsheetApp.flush();

  // Export as CSV
  systemLog(`[SERVER] Rendering CSV from sheet display values...`);
  let csvBlob = buildCsvBlobFromSheet_(sheetToExport, csvName);
  systemLog(`[SERVER] CSV Blob generated. Writing to 02_Ready folder...`);
  readyFolder.createFile(csvBlob);

  // Clean up files
  systemLog(`[SERVER] Cleaning up origin files...`);
  DriveApp.getFileById(tempSheetId).setTrashed(true);
  file.setTrashed(true);
  systemLog(`[SERVER] Cleanup complete. Process finished.`);

  return { success: true, log: `[SUCCESS] Converted: ${fileObj.name} -> ${csvName}`, trace: serverTrace };
}

function worksheetToCsvPreservingRawValues_(worksheet) {
  const ref = worksheet['!ref'];
  if (!ref) return '';

  const range = XLSX.utils.decode_range(ref);
  const rows = [];

  for (let rowIndex = range.s.r; rowIndex <= range.e.r; rowIndex++) {
    const values = [];
    for (let colIndex = range.s.c; colIndex <= range.e.c; colIndex++) {
      const address = XLSX.utils.encode_cell({ r: rowIndex, c: colIndex });
      const cell = worksheet[address];
      values.push(escapeCsvValue_(sheetJsRawCellValue_(cell, colIndex > range.s.c)));
    }
    rows.push(values.join(','));
  }

  return rows.join('\n');
}

function sheetJsRawCellValue_(cell, enforceMinimumDecimals) {
  if (!cell || cell.v == null) return '';

  if (cell.t === 'n') {
    return enforceMinimumDecimals ? formatNumberAtLeast5Decimals_(cell.v) : String(cell.v);
  }
  if (cell.t === 'b') return cell.v ? 'TRUE' : 'FALSE';
  if (cell.t === 'd' && cell.v instanceof Date) return cell.v.toISOString();

  return String(cell.v);
}

function applyMinimumDecimalFormatExceptFirstColumn_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow === 0 || lastColumn <= 1) {
    return;
  }

  sheet.getRange(1, 2, lastRow, lastColumn - 1).setNumberFormat('0.00000###############');
}

function buildCsvBlobFromSheet_(sheet, fileName) {
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow === 0 || lastColumn === 0) {
    return Utilities.newBlob('', MimeType.CSV, fileName);
  }

  const values = sheet.getRange(1, 1, lastRow, lastColumn).getDisplayValues();
  const csvString = values.map(row => row.map(escapeCsvValue_).join(',')).join('\n');
  return Utilities.newBlob(csvString, MimeType.CSV, fileName);
}

function formatNumberAtLeast5Decimals_(num) {
  let str = String(num);

  // Expand scientific notation (e.g. 1e-7 or 1.23e+5) into plain decimal form
  if (/[eE]/.test(str)) {
    const [coeff, expPart] = str.toLowerCase().split('e');
    const exp = parseInt(expPart, 10);
    const [intStr, decStr = ''] = coeff.split('.');
    const isNeg = intStr.startsWith('-');
    const absInt = intStr.replace('-', '');
    const allDigits = absInt + decStr;
    const newDotPos = absInt.length + exp; // where the decimal point lands

    if (newDotPos <= 0) {
      // e.g. 1.23e-3 → 0.00123
      str = (isNeg ? '-' : '') + '0.' + '0'.repeat(-newDotPos) + allDigits;
    } else if (newDotPos >= allDigits.length) {
      // e.g. 1.23e+5 → 123000
      str = (isNeg ? '-' : '') + allDigits + '0'.repeat(newDotPos - allDigits.length);
    } else {
      // e.g. 1.234e+2 → 123.4
      str = (isNeg ? '-' : '') + allDigits.slice(0, newDotPos) + '.' + allDigits.slice(newDotPos);
    }
  }

  // Pad to at least 5 decimal places
  const dotIdx = str.indexOf('.');
  if (dotIdx === -1) {
    return str + '.00000';
  }
  const currentDecimals = str.length - dotIdx - 1;
  if (currentDecimals < 5) {
    return str + '0'.repeat(5 - currentDecimals);
  }
  return str;
}

function escapeCsvValue_(value) {
  const text = String(value == null ? '' : value);
  if (/[",\n\r]/.test(text)) {
    return '"' + text.replace(/"/g, '""') + '"';
  }
  return text;
}