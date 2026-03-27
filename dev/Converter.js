// ============================================================================
// CONFIGURATION
// ============================================================================
const UPLOADS_FOLDER_ID = '1ecRiWJON03Pd0qDNpRxNhTNDsYyw614Z';
const READY_FOLDER_ID = '16mMxz1DvsIEgKUk4mAXnamwxIQ50ddP5';
const LARGE_FILE_STAGE_THRESHOLD_BYTES = 50 * 1024 * 1024;
const SANITIZED_ERROR_CELL_VALUE = '';
const DATE_DAY_OFFSET = 1;
const ERROR_CELL_LOG_LIMIT = 200;
const ERROR_CELL_LOG_FALLBACK_DISPLAY = '(blank-display)';
const CHUNK_MAX_ROWS = 200000;
const SHEETJS_MAX_FILE_BYTES = 100 * 1024 * 1024; // SheetJS + Uint8Array must fit in V8's ~256 MB heap

const SPREADSHEET_ERROR_MARKERS = [
  '#ERROR!',
  '#REF!',
  '#VALUE!',
  '#N/A',
  '#DIV/0!',
  '#NAME?',
  '#NUM!',
  '#NULL!',
  '#WERT!',
  '#BEZUG!',
  '#NV',
  '#ZAHL!'
];

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
  const menu = ui.createMenu('Makro');
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
  addIfAvailable('Execute transformations (Lagerliste)', 'openTransformUI');

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
  const fileSize = Number(file.getSize() || 0);
  
  let logMessage = "";
  let serverTrace = []; 
  
  // Dual-logger: Writes to Apps Script Executions AND the UI array
  function systemLog(msg) {
    console.log(msg);
    serverTrace.push(msg);
  }
  
  systemLog(`[SERVER] --- STARTING FILE: ${fileObj.name} ---`);
  systemLog(`[SERVER] Received ID: ${fileObj.id} | Type: ${fileObj.mimeType}`);
  systemLog(`[SERVER] File size: ${fileSize} bytes`);
  
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
      let isRwaFile = lowerName.includes('rwa');
      let isLargeFile = fileSize > LARGE_FILE_STAGE_THRESHOLD_BYTES;

      // --- PATH 1: SHEETJS IN-MEMORY PARSING (Heavy/binary files under size threshold) ---
      if ((isHeavyFile || isXlsb) && !isLargeFile) {
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
            return convertViaDrivePath_(file, fileObj, csvName, readyFolder, UPLOADS_FOLDER_ID, systemLog, serverTrace, {
              keepAsGoogleSheet: false,
              forceRwaDecimalStrings: isRwaFile
            });
          }
          throw heavyError;
        }
      }

      // --- PATH 2: CHUNKED CONVERSION (Large files > 50 MB) ---
      else if (isLargeFile) {
        systemLog(`[SERVER] Large file detected (${Math.round(fileSize / (1024 * 1024))}MB > ${Math.round(LARGE_FILE_STAGE_THRESHOLD_BYTES / (1024 * 1024))}MB). Will produce chunked CSV output.`);

        // Try SheetJS chunked parsing — but only when the file fits in V8 memory.
        // Above SHEETJS_MAX_FILE_BYTES the Uint8Array + parsed workbook would OOM the runtime.
        if ((isHeavyFile || isXlsb) && fileSize <= SHEETJS_MAX_FILE_BYTES) {
          try {
            systemLog(`[SERVER] Attempting SheetJS chunked parsing for heavy/binary file...`);
            let csvBlobs = convertHeavyExcelInChunks_(fileObj.id, csvName, { forceRwaDecimalStrings: isRwaFile });
            csvBlobs.forEach(function(b) { readyFolder.createFile(b); });
            file.setTrashed(true);
            let chunkNames = csvBlobs.map(function(b) { return b.getName(); }).join(', ');
            systemLog(`[SERVER] SheetJS chunked conversion complete: ${chunkNames}`);
            return { success: true, log: `[SUCCESS] SheetJS Chunked: ${fileObj.name} -> ${chunkNames}`, trace: serverTrace };
          } catch (sheetJsError) {
            const sjMsg = String(sheetJsError && sheetJsError.message || sheetJsError);
            if (isXlsb) {
              systemLog(`[SERVER] SheetJS chunked parsing failed for XLSB: ${sjMsg}`);
              return { success: false, log: `[ERROR] ${fileObj.name}: XLSB chunked parsing failed: ${sjMsg}`, trace: serverTrace };
            }
            systemLog(`[SERVER] SheetJS chunked failed (${sjMsg}). Falling back to Drive conversion with chunked export...`);
          }
        }

        // Drive conversion + chunked CSV export
        if (fileSize > SHEETJS_MAX_FILE_BYTES) {
          systemLog(`[SERVER] File exceeds SheetJS memory-safe limit (${Math.round(SHEETJS_MAX_FILE_BYTES / (1024 * 1024))}MB). Using server-side Drive conversion.`);
        }
        return convertViaDrivePath_(file, fileObj, csvName, readyFolder, UPLOADS_FOLDER_ID, systemLog, serverTrace, {
          keepAsGoogleSheet: false,
          forceRwaDecimalStrings: isRwaFile,
          chunked: true,
          skipBlobFallback: fileSize > LARGE_FILE_STAGE_THRESHOLD_BYTES
        });
      }
      
      // --- PATH 3: STANDARD GOOGLE DRIVE API CONVERSION ---
      else {
        return convertViaDrivePath_(file, fileObj, csvName, readyFolder, UPLOADS_FOLDER_ID, systemLog, serverTrace, {
          keepAsGoogleSheet: false,
          forceRwaDecimalStrings: isRwaFile
        });
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
      return { success: false, log: `[ERROR] ${fileObj.name}: File exceeded Apps Script conversion limits. Stage it as a Google Sheet or process it with an external converter.`, trace: serverTrace };
    }
    return { success: false, log: `[ERROR] ${fileObj.name}: ${error.message}`, trace: serverTrace };
  }
}

// ============================================================================
// 4. HELPER FUNCTIONS
// ============================================================================
function convertToGoogleSheet_(excelFile, folderId, options) {
  const settings = options || {};
  const metadata = { name: excelFile.getName(), mimeType: MimeType.GOOGLE_SHEETS, parents: [folderId] };

  // Path A: Copy+convert with retries (fastest, but can return transient "Internal Error")
  console.log('[CONVERT-GS] Path A: Attempting Drive.Files.copy() ...');
  let lastErr;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      console.log('[CONVERT-GS] copy() attempt ' + attempt + ' ...');
      const newFile = Drive.Files.copy(metadata, excelFile.getId(), { supportsAllDrives: true });
      console.log('[CONVERT-GS] copy() succeeded. New file ID: ' + newFile.id);
      return newFile.id;
    } catch (e) {
      lastErr = e;
      const msg = String(e && e.message || e);
      console.log('[CONVERT-GS] copy() attempt ' + attempt + ' failed: ' + msg);
      const transient = /Internal Error|backendError|rate limit|timeout/i.test(msg);
      if (!transient || attempt === 4) break;
      Utilities.sleep(1500 * attempt);
    }
  }

  // Path B: Resumable upload with server-side conversion (for large files that exceed copy limits).
  // Streams 25 MB chunks from the source file so memory stays low.
  if (settings.skipBlobFallback) {
    console.log('[CONVERT-GS] Path B: skipBlobFallback=true → entering resumable upload path ...');
    try {
      const resultId = convertLargeExcelViaResumableUpload_(excelFile.getId(), excelFile.getName(), folderId);
      console.log('[CONVERT-GS] Resumable upload succeeded. New file ID: ' + resultId);
      return resultId;
    } catch (resumableErr) {
      const primaryMsg  = String(lastErr && lastErr.message || lastErr || 'unknown');
      const resumableMsg = String(resumableErr && resumableErr.message || resumableErr || 'unknown');
      console.log('[CONVERT-GS] Resumable upload failed: ' + resumableMsg);
      throw new Error(`Drive conversion failed. copy() error: ${primaryMsg} | resumable upload error: ${resumableMsg}`);
    }
  }

  // Path C: Fallback to insert+convert using media blob (smaller files only).
  console.log('[CONVERT-GS] Path C: Attempting blob insert/create fallback ...');
  try {
    let inserted;
    try {
      inserted = Drive.Files.create(metadata, excelFile.getBlob(), { supportsAllDrives: true });
    } catch (v3Err) {
      if (/is not a function|not defined|cannot read/i.test(String(v3Err))) {
        inserted = Drive.Files.insert(metadata, excelFile.getBlob(), { convert: true, supportsAllDrives: true });
      } else {
        throw v3Err;
      }
    }
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
  const isRwaFile = String(csvFileName || '').toLowerCase().includes('rwa');
  const csvString = worksheetToCsvPreservingRawValues_(worksheet, {
    forceRwaDecimalStrings: isRwaFile
  }); 
  
  // 7. Package it as a Blob ready to be saved
  return Utilities.newBlob(csvString, MimeType.CSV, csvFileName);
}

/**
 * Downloads a Drive file as a Uint8Array using byte-range requests.
 * Each request fetches ≤ 25 MB, staying within UrlFetchApp's response-size
 * ceiling and bypassing the ~50 MB limit of DriveApp.getBlob().
 */
function downloadLargeFileBytes_(fileId) {
  // DriveApp.getSize() works regardless of which Drive advanced service version is enabled.
  const fileSize = Number(DriveApp.getFileById(fileId).getSize() || 0);
  if (!fileSize) throw new Error('Cannot determine file size for range download.');

  const RANGE_CHUNK = 25 * 1024 * 1024;
  const url = 'https://www.googleapis.com/drive/v3/files/' + fileId + '?alt=media';
  const token = ScriptApp.getOAuthToken();
  const result = new Uint8Array(fileSize);
  let offset = 0;

  while (offset < fileSize) {
    const end = Math.min(offset + RANGE_CHUNK - 1, fileSize - 1);
    const resp = UrlFetchApp.fetch(url, {
      headers: { 'Authorization': 'Bearer ' + token, 'Range': 'bytes=' + offset + '-' + end },
      muteHttpExceptions: true
    });
    const code = resp.getResponseCode();
    if (code !== 206 && code !== 200) {
      throw new Error('Range download failed at offset ' + offset + ': HTTP ' + code);
    }
    const chunk = resp.getContent();
    // Uint8Array assignment auto-converts signed Java bytes to unsigned.
    for (let i = 0; i < chunk.length; i++) {
      result[offset + i] = chunk[i];
    }
    offset += chunk.length;
  }

  return result;
}

/**
 * Converts a large Excel file to a Google Sheet using the Drive v3 resumable
 * upload API with server-side conversion.  The source file is streamed in
 * 25 MB chunks (download-from-Drive → upload-to-new-Sheet) so the Apps Script
 * runtime never holds the full file in memory (~50 MB peak).
 *
 * The resumable upload conversion limit for xlsx is ~200 MB – well above the
 * ~100 MB ceiling of Drive.Files.copy().
 *
 * @param {string} fileId   Drive file ID of the source xlsx.
 * @param {string} fileName Display name for the new Google Sheet.
 * @param {string} folderId Drive folder to create the Sheet in.
 * @returns {string} The ID of the newly created Google Sheet.
 */
function convertLargeExcelViaResumableUpload_(fileId, fileName, folderId) {
  console.log('[RESUMABLE] Starting resumable upload conversion for ' + fileName + ' (fileId=' + fileId + ')');
  const token = ScriptApp.getOAuthToken();
  const fileSize = Number(DriveApp.getFileById(fileId).getSize() || 0);
  console.log('[RESUMABLE] File size: ' + fileSize + ' bytes (' + Math.round(fileSize / (1024 * 1024)) + ' MB)');
  if (!fileSize) throw new Error('Cannot determine file size for resumable upload.');

  // --- Step 1: Initiate a resumable upload session with conversion ---
  console.log('[RESUMABLE] Step 1: Initiating resumable upload session ...');
  const metadataPayload = JSON.stringify({
    name: fileName,
    mimeType: 'application/vnd.google-apps.spreadsheet',
    parents: [folderId]
  });

  const initResp = UrlFetchApp.fetch(
    'https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true',
    {
      method: 'post',
      contentType: 'application/json; charset=UTF-8',
      headers: {
        'Authorization': 'Bearer ' + token,
        'X-Upload-Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'X-Upload-Content-Length': String(fileSize)
      },
      payload: metadataPayload,
      muteHttpExceptions: true
    }
  );

  const initCode = initResp.getResponseCode();
  console.log('[RESUMABLE] Session init response: HTTP ' + initCode);
  if (initCode !== 200) {
    const body = initResp.getContentText();
    console.log('[RESUMABLE] Session init FAILED body: ' + body.substring(0, 500));
    throw new Error('Resumable session init HTTP ' + initCode + ': ' + body);
  }

  // Retrieve the upload URI (case-insensitive header lookup)
  const respHeaders = initResp.getHeaders();
  let uploadUrl = '';
  for (var hKey in respHeaders) {
    if (hKey.toLowerCase() === 'location') { uploadUrl = respHeaders[hKey]; break; }
  }
  if (!uploadUrl) throw new Error('No upload URI returned from resumable session.');
  console.log('[RESUMABLE] Upload URI obtained (length=' + uploadUrl.length + ')');

  // --- Step 2: Stream 25 MB chunks from the source file to the upload URI ---
  // 25 MB is a multiple of 256 KiB (required by the resumable protocol) and
  // stays safely within UrlFetchApp's ~50 MB request/response ceiling.
  const CHUNK = 25 * 1024 * 1024;
  const downloadUrl = 'https://www.googleapis.com/drive/v3/files/' + fileId + '?alt=media';
  let offset = 0;
  let chunkNum = 0;
  const totalChunks = Math.ceil(fileSize / CHUNK);
  console.log('[RESUMABLE] Step 2: Streaming ' + totalChunks + ' chunks of ' + Math.round(CHUNK / (1024 * 1024)) + ' MB each ...');

  while (offset < fileSize) {
    chunkNum++;
    const end = Math.min(offset + CHUNK - 1, fileSize - 1);
    const expectedLen = end - offset + 1;
    const chunkSizeMB = Math.round(expectedLen / (1024 * 1024));

    // Download this byte range from the source file.
    // Use getBlob() instead of getContent() so the payload stays in Java heap
    // (outside V8's ~256 MB limit) – avoids OOM on large multi-chunk uploads.
    console.log('[RESUMABLE] Chunk ' + chunkNum + '/' + totalChunks + ': downloading bytes ' + offset + '-' + end + ' (' + chunkSizeMB + ' MB) ...');
    var dlResp = UrlFetchApp.fetch(downloadUrl, {
      headers: { 'Authorization': 'Bearer ' + token, 'Range': 'bytes=' + offset + '-' + end },
      muteHttpExceptions: true
    });
    const dlCode = dlResp.getResponseCode();
    if (dlCode !== 206 && dlCode !== 200) {
      console.log('[RESUMABLE] Chunk ' + chunkNum + ' download FAILED: HTTP ' + dlCode);
      throw new Error('Resumable source download failed at offset ' + offset + ': HTTP ' + dlCode);
    }
    var dlBlob = dlResp.getBlob();
    console.log('[RESUMABLE] Chunk ' + chunkNum + ' downloaded (' + chunkSizeMB + ' MB). Uploading ...');
    dlResp = null; // release response reference for GC

    // Upload this range to the resumable session
    const contentRange = 'bytes ' + offset + '-' + (offset + expectedLen - 1) + '/' + fileSize;
    var ulResp = UrlFetchApp.fetch(uploadUrl, {
      method: 'put',
      contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      headers: {
        'Content-Range': contentRange
      },
      payload: dlBlob,
      muteHttpExceptions: true
    });
    dlBlob = null; // release blob reference for GC

    const ulCode = ulResp.getResponseCode();
    console.log('[RESUMABLE] Chunk ' + chunkNum + ' upload response: HTTP ' + ulCode);
    if (ulCode === 200 || ulCode === 201) {
      // Final chunk accepted – file created and converted
      var result = JSON.parse(ulResp.getContentText());
      console.log('[RESUMABLE] Upload COMPLETE. New file ID: ' + result.id);
      return result.id;
    } else if (ulCode === 308) {
      // Chunk accepted, continue with next range
      offset += expectedLen;
      console.log('[RESUMABLE] Chunk ' + chunkNum + ' accepted. Next offset: ' + offset);
    } else {
      const ulBody = ulResp.getContentText();
      console.log('[RESUMABLE] Chunk ' + chunkNum + ' upload FAILED: HTTP ' + ulCode + ' body=' + ulBody.substring(0, 500));
      throw new Error('Resumable upload failed at offset ' + offset + ': HTTP ' + ulCode + ' ' + ulBody);
    }
    ulResp = null; // release upload response for GC
    Utilities.sleep(50); // yield to runtime for GC
  }

  throw new Error('Resumable upload finished all chunks without receiving a completion response.');
}

/**
 * Like convertHeavyExcelWithSheetJS_ but splits the output into multiple
 * CSV blobs of at most CHUNK_MAX_ROWS data rows each (headers repeated).
 * Uses byte-range downloads so files larger than 50 MB are supported.
 * Returns an array of Blobs.
 */
function convertHeavyExcelInChunks_(fileId, csvFileName, options) {
  const settings = options || {};

  const sheetJSUrl = "https://cdn.sheetjs.com/xlsx-0.19.3/package/dist/xlsx.full.min.js";
  const scriptText = UrlFetchApp.fetch(sheetJSUrl).getContentText();
  eval(scriptText);
  globalThis.XLSX = XLSX;

  // Use range download to bypass the ~50 MB getBlob() limit.
  const u8 = downloadLargeFileBytes_(fileId);
  const workbook = XLSX.read(u8, { type: 'array', cellDates: true });

  const firstSheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[firstSheetName];

  return buildChunkedCsvFromWorksheet_(worksheet, csvFileName, settings);
}

/**
 * Splits a SheetJS worksheet into multiple CSV blobs (each with headers).
 */
function buildChunkedCsvFromWorksheet_(worksheet, baseFileName, options) {
  const settings = options || {};
  const ref = worksheet['!ref'];
  if (!ref) return [Utilities.newBlob('', MimeType.CSV, baseFileName)];

  const range = XLSX.utils.decode_range(ref);

  // Build header line from first row
  const headerValues = [];
  for (let c = range.s.c; c <= range.e.c; c++) {
    const addr = XLSX.utils.encode_cell({ r: range.s.r, c: c });
    const cell = worksheet[addr];
    headerValues.push(escapeCsvValue_(sheetJsRawCellValue_(cell, c > range.s.c, settings)));
  }
  const headerLine = headerValues.join(',');

  const dataStartRow = range.s.r + 1;
  const totalDataRows = range.e.r - dataStartRow + 1;
  if (totalDataRows <= 0) return [Utilities.newBlob(headerLine, MimeType.CSV, baseFileName)];

  const chunkCount = Math.max(1, Math.ceil(totalDataRows / CHUNK_MAX_ROWS));
  const blobs = [];

  for (let chunk = 0; chunk < chunkCount; chunk++) {
    const startRow = dataStartRow + (chunk * CHUNK_MAX_ROWS);
    const endRow = Math.min(startRow + CHUNK_MAX_ROWS - 1, range.e.r);

    const rows = [headerLine];
    for (let r = startRow; r <= endRow; r++) {
      const values = [];
      for (let c = range.s.c; c <= range.e.c; c++) {
        const addr = XLSX.utils.encode_cell({ r: r, c: c });
        const cell = worksheet[addr];
        values.push(escapeCsvValue_(sheetJsRawCellValue_(cell, c > range.s.c, settings)));
      }
      rows.push(values.join(','));
    }

    const chunkName = chunkCount === 1
      ? baseFileName
      : baseFileName.replace(/\.csv$/i, '') + '__chunk_' + (chunk + 1) + '.csv';

    blobs.push(Utilities.newBlob(rows.join('\n'), MimeType.CSV, chunkName));
  }

  return blobs;
}

/**
 * Reads a Google Sheet in row batches and returns multiple CSV blobs
 * (each with headers) to avoid holding the entire sheet in memory.
 */
function buildChunkedCsvBlobsFromSheet_(sheet, baseFileName, options) {
  const settings = options || {};
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow === 0 || lastColumn === 0) {
    return [Utilities.newBlob('', MimeType.CSV, baseFileName)];
  }

  const timezone = resolveSheetTimeZone_(sheet);

  // Read and format header row
  const headerRange = sheet.getRange(1, 1, 1, lastColumn);
  const headerValues = headerRange.getValues()[0];
  const headerFormats = headerRange.getNumberFormats()[0];
  const headerCsv = headerValues
    .map(function (v, i) { return escapeCsvValue_(normalizeSheetValueForCsv_(v, headerFormats[i], i > 0, timezone, settings)); })
    .join(',');

  const totalDataRows = lastRow - 1;
  if (totalDataRows <= 0) return [Utilities.newBlob(headerCsv, MimeType.CSV, baseFileName)];

  const chunkCount = Math.max(1, Math.ceil(totalDataRows / CHUNK_MAX_ROWS));
  const blobs = [];

  for (let chunk = 0; chunk < chunkCount; chunk++) {
    const startRow = 2 + (chunk * CHUNK_MAX_ROWS);
    const rowsInChunk = Math.min(CHUNK_MAX_ROWS, lastRow - startRow + 1);
    if (rowsInChunk <= 0) break;

    const dataRange = sheet.getRange(startRow, 1, rowsInChunk, lastColumn);
    const values = dataRange.getValues();
    const formats = dataRange.getNumberFormats();

    const lines = [headerCsv];
    for (let r = 0; r < values.length; r++) {
      lines.push(
        values[r]
          .map(function (v, c) { return escapeCsvValue_(normalizeSheetValueForCsv_(v, formats[r][c], c > 0, timezone, settings)); })
          .join(',')
      );
    }

    const chunkName = chunkCount === 1
      ? baseFileName
      : baseFileName.replace(/\.csv$/i, '') + '__chunk_' + (chunk + 1) + '.csv';

    blobs.push(Utilities.newBlob(lines.join('\n'), MimeType.CSV, chunkName));
  }

  return blobs;
}

function convertViaDrivePath_(file, fileObj, csvName, readyFolder, folderId, systemLog, serverTrace, options) {
  const settings = options || {};
  systemLog(`[SERVER] Instructing Google Drive to convert Excel file...`);
  let tempSheetId = convertToGoogleSheet_(file, folderId, {
    skipBlobFallback: !!settings.skipBlobFallback || !!settings.keepAsGoogleSheet
  });
  systemLog(`[SERVER] Drive API success. Temp Sheet ID: ${tempSheetId}`);

  systemLog(`[SERVER] Opening sheet to extract data...`);
  let spreadsheet = SpreadsheetApp.openById(tempSheetId);
  let sheetToExport = spreadsheet.getSheets()[0];

  systemLog(`[SERVER] Targeted first tab: [${sheetToExport.getName()}]`);
  systemLog(`[SERVER] Timezones => Spreadsheet: ${spreadsheet.getSpreadsheetTimeZone()} | Script: ${Session.getScriptTimeZone()}`);

  systemLog(`[SERVER] Sanitizing spreadsheet error cells before downstream export...`);
  const sanitizeSummary = sanitizeSheetErrorCells_(sheetToExport, systemLog);
  const sanitizedErrorCells = sanitizeSummary.clearedCells;
  if (sanitizedErrorCells > 0) {
    SpreadsheetApp.flush();
    systemLog(`[SERVER] Replaced ${sanitizedErrorCells} error cells with blanks.`);

    // Drive conversion can break Excel-only formulas (_xlfn.*, external refs like [1]Sheet0, etc.).
    // For CSV output, retry via SheetJS to use workbook cached values where available.
    if (!settings.keepAsGoogleSheet && sanitizeSummary.hasFormulaErrors) {
      systemLog(`[SERVER] Formula-based spreadsheet errors detected after Drive conversion. Attempting SheetJS fallback for CSV integrity...`);
      try {
        const csvBlobFromSheetJs = convertHeavyExcelWithSheetJS_(fileObj.id, csvName);
        readyFolder.createFile(csvBlobFromSheetJs);

        DriveApp.getFileById(tempSheetId).setTrashed(true);
        file.setTrashed(true);
        systemLog(`[SERVER] SheetJS fallback succeeded. Created CSV using workbook cached values.`);

        return {
          success: true,
          log: `[SUCCESS] Recovered via SheetJS fallback: ${fileObj.name} -> ${csvName} (detected ${sanitizedErrorCells} formula error cells in Drive conversion)`,
          trace: serverTrace
        };
      } catch (sheetJsFallbackError) {
        systemLog(`[SERVER] SheetJS fallback failed: ${sheetJsFallbackError.message}. Continuing with sanitized Drive export.`);
      }
    }
  } else {
    systemLog(`[SERVER] No spreadsheet error markers found.`);
  }

  systemLog(`[SERVER] Applying minimum 5-decimal formatting to all columns except the first...`);
  applyMinimumDecimalFormatExceptFirstColumn_(sheetToExport);
  SpreadsheetApp.flush();

  // Export as CSV
  if (settings.chunked) {
    systemLog(`[SERVER] Rendering chunked CSV output (max ${CHUNK_MAX_ROWS} rows per chunk)...`);
    let csvBlobs = buildChunkedCsvBlobsFromSheet_(sheetToExport, csvName, {
      forceRwaDecimalStrings: !!settings.forceRwaDecimalStrings
    });
    csvBlobs.forEach(function(b) { readyFolder.createFile(b); });
    let chunkNames = csvBlobs.map(function(b) { return b.getName(); }).join(', ');
    systemLog(`[SERVER] Created ${csvBlobs.length} CSV chunk(s): ${chunkNames}`);

    // Clean up files
    systemLog(`[SERVER] Cleaning up origin files...`);
    DriveApp.getFileById(tempSheetId).setTrashed(true);
    file.setTrashed(true);
    systemLog(`[SERVER] Cleanup complete. Process finished.`);

    return { success: true, log: `[SUCCESS] Converted (chunked): ${fileObj.name} -> ${chunkNames}`, trace: serverTrace };
  }

  systemLog(`[SERVER] Rendering CSV from sheet display values...`);
  let csvBlob = buildCsvBlobFromSheet_(sheetToExport, csvName, {
    forceRwaDecimalStrings: !!settings.forceRwaDecimalStrings
  });
  systemLog(`[SERVER] CSV Blob generated. Writing to 02_Ready folder...`);
  readyFolder.createFile(csvBlob);

  // Clean up files
  systemLog(`[SERVER] Cleaning up origin files...`);
  DriveApp.getFileById(tempSheetId).setTrashed(true);
  file.setTrashed(true);
  systemLog(`[SERVER] Cleanup complete. Process finished.`);

  return { success: true, log: `[SUCCESS] Converted: ${fileObj.name} -> ${csvName}`, trace: serverTrace };
}

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
      const cell = worksheet[address];
      values.push(escapeCsvValue_(sheetJsRawCellValue_(cell, colIndex > range.s.c, settings)));
    }
    rows.push(values.join(','));
  }

  return rows.join('\n');
}

function sheetJsRawCellValue_(cell, enforceMinimumDecimals, options) {
  const settings = options || {};
  if (!cell || cell.v == null) return '';

  if (cell.t === 'e') return SANITIZED_ERROR_CELL_VALUE;

  if (cell.t === 'd' && cell.v instanceof Date) {
    const shiftedDate = shiftYmdByOffset_(cell.v.getUTCFullYear(), cell.v.getUTCMonth() + 1, cell.v.getUTCDate(), DATE_DAY_OFFSET);
    return formatDatePartsAsMdy_(shiftedDate.year, shiftedDate.month, shiftedDate.day);
  }

  if (cell.t === 'n' && isSheetJsDateCell_(cell)) {
    const parsedDate = parseSheetJsDateSerial_(cell.v);
    if (parsedDate) return parsedDate;
  }

  if (cell.t === 'n') {
    if (settings.forceRwaDecimalStrings) {
      return normalizeBigNumericString_(cell.v);
    }
    return enforceMinimumDecimals ? formatNumberAtLeast5Decimals_(cell.v) : String(cell.v);
  }
  if (cell.t === 'b') return cell.v ? 'TRUE' : 'FALSE';

  return sanitizeSpreadsheetErrorValue_(String(cell.v));
}

function applyMinimumDecimalFormatExceptFirstColumn_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow === 0 || lastColumn <= 1) {
    return;
  }

  sheet.getRange(1, 2, lastRow, lastColumn - 1).setNumberFormat('0.00000###############');
}

function buildCsvBlobFromSheet_(sheet, fileName, options) {
  const settings = options || {};
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow === 0 || lastColumn === 0) {
    return Utilities.newBlob('', MimeType.CSV, fileName);
  }

  const range = sheet.getRange(1, 1, lastRow, lastColumn);
  const values = range.getValues();
  const formats = range.getNumberFormats();
  const timezone = resolveSheetTimeZone_(sheet);
  const csvString = values
    .map((row, rowIdx) => row
      .map((value, colIdx) => escapeCsvValue_(normalizeSheetValueForCsv_(value, formats[rowIdx][colIdx], colIdx > 0, timezone, settings)))
      .join(','))
    .join('\n');
  return Utilities.newBlob(csvString, MimeType.CSV, fileName);
}

function sanitizeSheetErrorCells_(sheet, systemLog) {
  let clearedCells = 0;
  let loggedCells = 0;
  let hasFormulaErrors = false;

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow === 0 || lastCol === 0) return 0;

  const range = sheet.getRange(1, 1, lastRow, lastCol);
  const displayValues = range.getDisplayValues();
  const formulas = range.getFormulas();
  const a1ToClear = [];

  for (let r = 0; r < lastRow; r++) {
    for (let c = 0; c < lastCol; c++) {
      const displayValue = String(displayValues[r][c] == null ? '' : displayValues[r][c]).trim();
      const marker = detectSpreadsheetErrorMarker_(displayValue);
      if (!marker) continue;

      const cellRange = sheet.getRange(r + 1, c + 1);
      const formulaText = formulas[r][c] || '';
      if (formulaText) hasFormulaErrors = true;
      if (systemLog && loggedCells < ERROR_CELL_LOG_LIMIT) {
        const rowKey = String(displayValues[r][0] == null ? '' : displayValues[r][0]).trim();
        logSpreadsheetErrorCell_(cellRange, marker, systemLog, rowKey, formulaText);
        loggedCells++;
      }

      a1ToClear.push(cellRange.getA1Notation());
      clearedCells++;
    }
  }

  if (a1ToClear.length) {
    sheet.getRangeList(a1ToClear).clearContent();
  }

  if (systemLog && clearedCells > ERROR_CELL_LOG_LIMIT) {
    systemLog(`[SERVER][ERROR-CELL] Logged first ${ERROR_CELL_LOG_LIMIT} error cells out of ${clearedCells} total matches.`);
  }

  return {
    clearedCells: clearedCells,
    hasFormulaErrors: hasFormulaErrors
  };
}

function logSpreadsheetErrorCell_(cellRange, marker, systemLog, rowKey, formulaFromGrid) {
  const sheet = cellRange.getSheet();
  const a1 = cellRange.getA1Notation();
  const formula = formulaFromGrid || cellRange.getFormula();
  const displayValue = cellRange.getDisplayValue();
  const rawValue = cellRange.getValue();
  const numberFormat = cellRange.getNumberFormat();
  const effectiveDisplay = displayValue || marker || ERROR_CELL_LOG_FALLBACK_DISPLAY;
  const formulaPart = formula ? ` formula=${formula}` : ' formula=(none)';
  const rawPart = rawValue === '' ? ' raw=(blank)' : ` raw=${String(rawValue)}`;
  const fmtPart = numberFormat ? ` format=${numberFormat}` : ' format=(none)';
  const keyPart = rowKey ? ` rowKey=${rowKey}` : '';

  systemLog(
    `[SERVER][ERROR-CELL] ${sheet.getName()}!${a1} marker=${marker} display=${effectiveDisplay}${formulaPart}${rawPart}${fmtPart}${keyPart}`
  );
}

function detectSpreadsheetErrorMarker_(displayValue) {
  const token = String(displayValue == null ? '' : displayValue).trim().toUpperCase();
  if (!token) return '';
  if (SPREADSHEET_ERROR_MARKERS.indexOf(token) !== -1) return token;
  // Generic safety net for localized spreadsheet error tokens (e.g. #WERT!, #BEZUG!, #SPILL!).
  return /^#\S+$/.test(token) ? token : '';
}

function normalizeSheetValueForCsv_(value, numberFormat, enforceMinimumDecimals, timezone, options) {
  const settings = options || {};
  if (value == null) return '';
  if (value instanceof Date) return formatDateForCsv_(value, timezone);
  if (typeof value === 'number') {
    if (isDateLikeNumberFormat_(numberFormat)) {
      return formatGoogleSheetsSerialDate_(value);
    }
    if (settings.forceRwaDecimalStrings) {
      return normalizeBigNumericString_(value);
    }
    return enforceMinimumDecimals ? formatNumberAtLeast5Decimals_(value) : String(value);
  }
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
  return sanitizeSpreadsheetErrorValue_(String(value));
}

function isSheetJsDateCell_(cell) {
  if (!cell) return false;
  if (cell.t === 'd') return true;
  if (cell.t !== 'n') return false;
  if (!cell.z || !globalThis.XLSX || !XLSX.SSF || typeof XLSX.SSF.is_date !== 'function') return false;
  return XLSX.SSF.is_date(cell.z);
}

function parseSheetJsDateSerial_(serialValue) {
  if (!globalThis.XLSX || !XLSX.SSF || typeof XLSX.SSF.parse_date_code !== 'function') return '';
  const parsed = XLSX.SSF.parse_date_code(serialValue);
  if (!parsed || !parsed.y || !parsed.m || !parsed.d) return '';

  const shiftedDate = shiftYmdByOffset_(parsed.y, parsed.m, parsed.d, DATE_DAY_OFFSET);
  return formatDatePartsAsMdy_(shiftedDate.year, shiftedDate.month, shiftedDate.day);
}

function formatDateForCsv_(dateObj, timezone) {
  // Format in spreadsheet timezone to avoid day shifts from script/runtime timezone.
  const tz = timezone || Session.getScriptTimeZone() || 'UTC';
  const year = parseInt(Utilities.formatDate(dateObj, tz, 'yyyy'), 10);
  const month = parseInt(Utilities.formatDate(dateObj, tz, 'MM'), 10);
  const day = parseInt(Utilities.formatDate(dateObj, tz, 'dd'), 10);
  const shiftedDate = shiftYmdByOffset_(year, month, day, DATE_DAY_OFFSET);
  return formatDatePartsAsMdy_(shiftedDate.year, shiftedDate.month, shiftedDate.day);
}

function resolveSheetTimeZone_(sheet) {
  try {
    return sheet.getParent().getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'UTC';
  } catch (e) {
    return Session.getScriptTimeZone() || 'UTC';
  }
}

function isDateLikeNumberFormat_(numberFormat) {
  const fmt = String(numberFormat == null ? '' : numberFormat).toLowerCase();
  if (!fmt) return false;
  const hasDateToken = /(^|[^a-z])(d|dd|ddd|dddd|m|mm|mmm|mmmm|yy|yyyy)([^a-z]|$)/i.test(fmt);
  const hasTimeOnlyToken = /(^|[^a-z])(h|hh|s|ss)([^a-z]|$)/i.test(fmt) && !/(d|m|y)/i.test(fmt);
  return hasDateToken && !hasTimeOnlyToken;
}

function formatGoogleSheetsSerialDate_(serial) {
  if (typeof serial !== 'number' || !isFinite(serial)) return '';
  // Google Sheets date serial epoch aligns with 1899-12-30.
  const millis = Math.round((serial - 25569) * 86400000);
  const dt = new Date(millis);
  const shiftedDate = shiftYmdByOffset_(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate(), DATE_DAY_OFFSET);
  return formatDatePartsAsMdy_(shiftedDate.year, shiftedDate.month, shiftedDate.day);
}

function shiftYmdByOffset_(year, month, day, offsetDays) {
  const shifted = new Date(Date.UTC(year, month - 1, day + offsetDays));
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate()
  };
}

function normalizeBigNumericString_(value) {
  let str = String(value == null ? '' : value).trim();
  if (!str) return '';

  str = str.replace(/\s+/g, '');
  if (/[eE]/.test(str)) {
    str = formatNumberAtLeast5Decimals_(Number(str));
  }

  if (str.indexOf(',') !== -1 && str.indexOf('.') !== -1) {
    str = str.replace(/,/g, '');
  } else if (str.indexOf(',') !== -1) {
    str = str.replace(/\./g, '').replace(/,/g, '.');
  }

  return str;
}

function formatDatePartsAsMdy_(year, month, day) {
  const mm = String(month).padStart(2, '0');
  const dd = String(day).padStart(2, '0');
  const yyyy = String(year).padStart(4, '0');
  return `${mm}/${dd}/${yyyy}`;
}

function sanitizeSpreadsheetErrorValue_(value) {
  const text = String(value == null ? '' : value);
  return isSpreadsheetErrorMarker_(text) ? SANITIZED_ERROR_CELL_VALUE : text;
}

function isSpreadsheetErrorMarker_(value) {
  return SPREADSHEET_ERROR_MARKERS.indexOf(String(value == null ? '' : value).trim()) !== -1;
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