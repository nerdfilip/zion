// ============================================================================
// CONFIGURATION: BIGQUERY & FOLDERS
// ============================================================================
const GCP_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b'; 
const DATASET_ID = 'imports'; 
const ARCHIVE_FOLDER_ID = '1IOrUiTS_xXb69EBUcb8rqPSbUhQKjugO'; 
// const READY_FOLDER_ID = '16mMxz1DvsIEgKUk4mAXnamwxIQ50ddP5';

const FILE_RULES = [
  { keyword: "db abfrage",                     headerRow: 1, dataRow: 2, delimiter: ";" },
  { keyword: "übersicht überschneiderartikel", headerRow: 2, dataRow: 3 },
  { keyword: "bäf_de",                         headerRow: 7, dataRow: 9 }, 
  { keyword: "osnl",                           headerRow: 1, dataRow: 2 },
  { keyword: "rwa",                            headerRow: 3, dataRow: 4 }
];

const FILE_SPECIAL_OPTIONS = [
  {
    keyword: "artikelkette",
    keepColumnIndexes: [0, 1],
    typeOverrides: { artikelkette: "STRING" }
  },
  {
    keyword: "ganzjahresartikel",
    keepColumnIndexes: [0, 1, 2]
  },
  {
    keyword: "reporting",
    headerRow: 3,
    dataRow: 5,
    headerRowByColumn: { 0: 4 },
    typeOverrides: { standortcode: "STRING", 
                     kommentar: "STRING", 
                     kommentar_1: "STRING", 
                     kommentar_2: "STRING" 
                     }
  }
];

const TYPE_OVERRIDES = {
  "ian": "INT64",
  "ean": "INT64",
  "artikelnummer": "INT64",
  "article_number": "INT64",
  "stock": "INT64",
  "bestand": "INT64",
  "laenderspezifische_sap_nummern": "INT64",
  "abverkaufshorizont_nat": "INT64",
  "kopfartikel": "INT64",
  "summe_von_st_rwa": "BIGNUMERIC",
  "rwa_pro_st_ck": "BIGNUMERIC",
  "rwa_pro_stueck": "BIGNUMERIC",
  "aktions_vk": "NUMERIC",
  "sortiment_vk_lidl": "NUMERIC",
  "name": "STRING",
  "kw": "STRING"
};

const HEADER_TYPE_RULES = [
  {
    type: 'BIGNUMERIC',
    patterns: [
      /^summe_von_st_rwa$/,
      /^rwa_pro_st_ck$/,
      /^rwa_pro_stueck$/,
      /(^|_)rwa(_|$)/
    ]
  },
  {
    type: 'INT64',
    patterns: [
      /^ian$/,
      /^ean$/,
      /^artikelnummer(_\d+)?$/,
      /^article_number(_\d+)?$/,
      /^laenderspezifische_sap_nummern$/,
      /^abverkaufshorizont_nat$/,
      /^kopfartikel$/,
      /(^|_)(sap_nummer|sap_nummern)(_|$)/,
      /(^|_)(stock|bestand)(_|$)/
    ]
  },
  {
    type: 'DATE',
    patterns: [
      /(^|_)(datum|date)(_|$)/,
      /(^|_)(gueltig_ab|gueltig_bis)(_|$)/
    ]
  },
  {
    type: 'NUMERIC',
    patterns: [
      /(^|_)(preis|wert|kosten|vk|eur|volumen|umsatz|betrag)(_|$)/,
      /^aktions_vk$/,
      /^sortiment_vk_lidl$/
    ]
  },
  {
    type: 'BOOL',
    patterns: [
      /(^|_)(aktiv|active|flag|is_.*|bool)(_|$)/
    ]
  }
];

function inferDataTypeFromHeader_(headerName) {
  const key = String(headerName || '').toLowerCase();
  if (!key) return null;
  if (TYPE_OVERRIDES[key]) return TYPE_OVERRIDES[key];

  for (let i = 0; i < HEADER_TYPE_RULES.length; i++) {
    const rule = HEADER_TYPE_RULES[i];
    const patterns = rule.patterns || [];
    for (let j = 0; j < patterns.length; j++) {
      if (patterns[j].test(key)) return rule.type;
    }
  }

  return null;
}

function normalizeNumberish_(value, fileDelimiter) {
  let v = String(value || '').trim();
  if (!v) return '';
  v = v.replace(/[€$£\s]/g, '').replace(/[^0-9,.-]/g, '');
  if (!v) return '';

  if (fileDelimiter === ';') {
    return v.replace(/\./g, '').replace(/,/g, '.');
  }
  return v.replace(/,/g, '');
}

function isDateLikeValue_(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return false;

  // ISO variants with optional time (e.g. 2026-03-27 or 2026-03-27T11:22:33).
  if (/^\d{4}[-\/.]\d{1,2}[-\/.]\d{1,2}(?:[ T].*)?$/.test(raw)) return true;

  // Day-first / month-first variants using slash, dot or dash separators.
  if (/^\d{1,2}[-\/.]\d{1,2}[-\/.]\d{2,4}(?:[ T].*)?$/.test(raw)) return true;

  return false;
}

function inferDataTypeFromSamples_(samples, fileDelimiter) {
  const values = (samples || [])
    .map(v => String(v == null ? '' : v).trim())
    .filter(v => v && !/^null$/i.test(v) && v !== '-');

  if (!values.length) return 'STRING';

  let dateCount = 0;
  let boolCount = 0;
  let intCount = 0;
  let decimalCount = 0;
  let intOverflowCount = 0;

  const INT64_MIN_BI = BigInt('-9223372036854775808');
  const INT64_MAX_BI = BigInt('9223372036854775807');

  for (let i = 0; i < values.length; i++) {
    const raw = values[i];
    if (/^(true|false|yes|no|ja|nein|0|1)$/i.test(raw)) {
      boolCount++;
      continue;
    }

    if (isDateLikeValue_(raw)) {
      dateCount++;
      continue;
    }

    const normalized = normalizeNumberish_(raw, fileDelimiter);
    if (!normalized) continue;

    const n = Number(normalized);
    if (Number.isNaN(n)) continue;

    if (/^-?\d+$/.test(normalized)) {
      intCount++;

      // Keep integer-like IDs that exceed INT64 as STRING to prevent load crashes.
      const bi = BigInt(normalized);
      if (bi < INT64_MIN_BI || bi > INT64_MAX_BI) {
        intOverflowCount++;
      }
    } else {
      decimalCount++;
    }
  }

  const total = values.length;
  const numericCount = intCount + decimalCount;
  if (dateCount / total >= 0.85) return 'DATE';
  if (boolCount / total >= 0.9) return 'BOOL';
  if (numericCount / total >= 0.85) {
    if (intCount > 0 && decimalCount === 0 && intOverflowCount > 0) return 'STRING';
    return decimalCount > 0 ? 'NUMERIC' : 'INT64';
  }
  return 'STRING';
}

function resolveColumnType_(headerName, sampleValues, fileDelimiter) {
  const byHeader = inferDataTypeFromHeader_(headerName);
  if (byHeader) return byHeader;
  return inferDataTypeFromSamples_(sampleValues, fileDelimiter);
}

function resolveColumnTypeWithOverrides_(headerName, sampleValues, fileDelimiter, typeOverrides) {
  const key = String(headerName || '').toLowerCase();
  if (typeOverrides && typeOverrides[key]) return typeOverrides[key];
  return resolveColumnType_(headerName, sampleValues, fileDelimiter);
}

function getSpecialFileOptions_(lowerName) {
  for (let i = 0; i < FILE_SPECIAL_OPTIONS.length; i++) {
    if (lowerName.includes(FILE_SPECIAL_OPTIONS[i].keyword)) return FILE_SPECIAL_OPTIONS[i];
  }
  return null;
}

function columnIndexToA1_(columnIndex) {
  let index = Number(columnIndex);
  if (!Number.isFinite(index) || index < 0) return null;

  let result = '';
  while (index >= 0) {
    result = String.fromCharCode((index % 26) + 65) + result;
    index = Math.floor(index / 26) - 1;
  }
  return result;
}

function buildSheetRangeFromColumnIndexes_(indexes) {
  if (!indexes || !indexes.length) return null;

  let normalized = indexes
    .map(n => Number(n))
    .filter(n => Number.isInteger(n) && n >= 0)
    .sort((a, b) => a - b);

  if (!normalized.length) return null;

  for (let i = 1; i < normalized.length; i++) {
    if (normalized[i] !== normalized[i - 1] + 1) return null;
  }

  let start = columnIndexToA1_(normalized[0]);
  let end = columnIndexToA1_(normalized[normalized.length - 1]);
  if (!start || !end) return null;
  return `${start}:${end}`;
}

function buildRawHeaders_(parsedRows, lineRows, fileDelimiter, headerRow, headerRowByColumn, keepColumnIndexes) {
  let baseHeaders = parsedRows && parsedRows.length
    ? (parsedRows[headerRow - 1] || [])
    : String(lineRows[headerRow - 1] || '').split(fileDelimiter);

  let headers = baseHeaders.slice();

  if (headerRowByColumn) {
    Object.keys(headerRowByColumn).forEach(k => {
      let colIndex = Number(k);
      let rowNumber = Number(headerRowByColumn[k]);
      if (!Number.isInteger(colIndex) || !Number.isInteger(rowNumber) || rowNumber <= 0) return;

      let overrideRow = parsedRows && parsedRows.length
        ? (parsedRows[rowNumber - 1] || [])
        : String(lineRows[rowNumber - 1] || '').split(fileDelimiter);

      headers[colIndex] = overrideRow[colIndex] || '';
    });
  }

  if (keepColumnIndexes && keepColumnIndexes.length) {
    headers = keepColumnIndexes.map(idx => (headers[idx] != null ? headers[idx] : ''));
  }

  return headers;
}

// --- UPDATED: Now fetches both CSV and Google Sheets, groups chunked files ---
function getReadyFiles() {
  const folder = DriveApp.getFolderById(READY_FOLDER_ID);
  const files = folder.getFiles();
  let rawList = [];
  
  while (files.hasNext()) {
    let f = files.next();
    let mime = f.getMimeType();
    
    if (mime === MimeType.CSV || mime === MimeType.GOOGLE_SHEETS || mime === 'text/csv') {
      rawList.push({ id: f.getId(), name: f.getName(), mimeType: mime });
    }
  }
  return groupChunkedFiles_(rawList);
}

/**
 * Groups __chunk_N files by their base name so they are processed as one
 * logical import. Standalone files pass through unchanged.
 */
function groupChunkedFiles_(fileList) {
  var CHUNK_PATTERN = /__chunk_(\d+)\.csv$/i;
  var groups = {};
  var standalone = [];

  for (var i = 0; i < fileList.length; i++) {
    var f = fileList[i];
    var match = f.name.match(CHUNK_PATTERN);

    if (match) {
      var baseName = f.name.replace(CHUNK_PATTERN, '.csv');
      if (!groups[baseName]) groups[baseName] = [];
      groups[baseName].push({ file: f, index: parseInt(match[1], 10) });
    } else {
      standalone.push(f);
    }
  }

  var result = standalone.slice();

  Object.keys(groups).forEach(function (baseName) {
    var sorted = groups[baseName].sort(function (a, b) { return a.index - b.index; });
    var parts = sorted.map(function (s) { return s.file; });
    result.push({
      id: parts[0].id,
      name: baseName,
      mimeType: parts[0].mimeType,
      parts: parts
    });
  });

  return result;
}

// ============================================================================
// 1. UI TRIGGER & UTILITIES
// ============================================================================
function openBQProgressUI() {
  const html = HtmlService.createHtmlOutputFromFile('BQProgressUI')
    .setWidth(600)
    .setHeight(500)
    .setTitle('BigQuery Ingestion Terminal');
  SpreadsheetApp.getUi().showModalDialog(html, 'Database Loader');
}

function cleanTableName(fileName) {
  // Strip common extensions just in case
  let raw = fileName.replace(/\.csv$/i, '').replace(/\.xlsx?$/i, '').replace(/\.xlsb$/i, '');
  const map = { 'ä':'ae', 'ö':'oe', 'ü':'ue', 'Ä':'ae', 'Ö':'oe', 'Ü':'ue', 'ß':'ss' };
  let en = raw.replace(/[äöüÄÖÜß]/g, m => map[m]);
  return 'raw_' + en.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
}

// ============================================================================
// 2. SCHEMA DETECTOR WITH SMART PROFILING
// ============================================================================
// --- UPDATED: Added mimeType parameter to handle Google Sheets ---
function buildDynamicSchema(fileId, headerRow, dataRow, forcedDelimiter, projectId, datasetId, mimeType, specialOptions) {
  console.log(`[SCHEMA] Phase 1: Fetching file ID ${fileId} from Drive...`);
  
  let url;
  if (mimeType === MimeType.GOOGLE_SHEETS) {
    // If it's a Google Sheet, we ask Google to export it as a CSV string on the fly
    url = `https://docs.google.com/spreadsheets/d/${fileId}/export?format=csv`;
  } else {
    // Standard CSV media download
    url = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`;
  }

  let response = UrlFetchApp.fetch(url, {
    headers: { 'Authorization': 'Bearer ' + ScriptApp.getOAuthToken(), 'Range': 'bytes=0-500000' },
    muteHttpExceptions: true
  });
  
  console.log(`[SCHEMA] Phase 2: Parsing CSV text...`);
  let rawText = response.getContentText();
  let lines = rawText.split(/\r?\n/);
  const opts = specialOptions || {};
  const keepColumnIndexes = opts.keepColumnIndexes || null;
  const headerRowByColumn = opts.headerRowByColumn || null;
  const typeOverrides = opts.typeOverrides || null;
  
  // Smart Delimiter Detection
  let fileDelimiter = forcedDelimiter;
  if (!fileDelimiter) {
    let firstLine = lines[0] || "";
    let commaCount = (firstLine.match(/,/g) || []).length;
    let semiCount = (firstLine.match(/;/g) || []).length;
    fileDelimiter = semiCount > commaCount ? ';' : ',';
    console.log(`[SCHEMA] Auto-detected delimiter: [${fileDelimiter}]`);
  }

  let rawHeaders = [];
  let sampleRows = [];
  try { 
    let parsed = Utilities.parseCsv(rawText, fileDelimiter); 
    rawHeaders = buildRawHeaders_(parsed, lines, fileDelimiter, headerRow, headerRowByColumn, keepColumnIndexes);
    sampleRows = parsed.slice(dataRow - 1, Math.min(parsed.length, dataRow - 1 + 40));
    if (keepColumnIndexes && keepColumnIndexes.length) {
      sampleRows = sampleRows.map(r => keepColumnIndexes.map(idx => (r && r[idx] != null ? r[idx] : '')));
    }
  } catch(e) { 
    rawHeaders = buildRawHeaders_(null, lines, fileDelimiter, headerRow, headerRowByColumn, keepColumnIndexes);
    let fallback = (lines[dataRow - 1] || "").split(fileDelimiter);
    if (keepColumnIndexes && keepColumnIndexes.length) {
      fallback = keepColumnIndexes.map(idx => (fallback[idx] != null ? fallback[idx] : ''));
    }
    sampleRows = [fallback];
  }

  console.log(`[SCHEMA] Phase 3: Translating headers to BigQuery format...`);
  const map = { 'ä':'ae', 'ö':'oe', 'ü':'ue', 'Ä':'ae', 'Ö':'oe', 'Ü':'ue', 'ß':'ss' };
  let englishHeaders = rawHeaders.map(val => {
    let en = String(val).replace(/[äöüÄÖÜß]/g, m => map[m]);
    en = en.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase().replace(/_+/g, '_').replace(/^_|_$/g, '');
    if (!en || /^[0-9]/.test(en)) en = 'col_' + en;
    return en.substring(0, 290);
  });

  console.log(`[SCHEMA] Phase 4: Deduplicating headers...`);
  let used = new Set();
  for(let i = 0; i < englishHeaders.length; i++) {
    let f = englishHeaders[i];
    let c = 1;
    while(used.has(f) && c < 500) { f = englishHeaders[i] + '_' + c; c++; }
    used.add(f);
    englishHeaders[i] = f;
  }

  console.log(`[SCHEMA] Phase 5: Resolving datatypes from mappings + profiling...`);
  let finalSchemaFields = [];
  
  for(let i = 0; i < englishHeaders.length; i++) {
    let colName = englishHeaders[i];
    let sampleValues = sampleRows.map(r => (r && r[i] != null ? r[i] : ''));
    let detectedType = resolveColumnTypeWithOverrides_(colName, sampleValues, fileDelimiter, typeOverrides);
    if (TYPE_OVERRIDES[colName]) {
      console.log(`   -> Exact override: ${colName} => ${detectedType}`);
    }
    
    finalSchemaFields.push({ name: colName, type: detectedType });
  }
  
  console.log(`[SCHEMA] Schema successfully built!`);
  return { schema: finalSchemaFields, delimiter: fileDelimiter };
}

// ============================================================================
// 3. PROCESS SINGLE FILE WITH VERBOSE LOGGING AND SQL CASCADE
// ============================================================================
function processSingleBQFile(fileObj) {
  const archiveFolder = DriveApp.getFolderById(ARCHIVE_FOLDER_ID);
  const file = DriveApp.getFileById(fileObj.id);
  const lowerName = fileObj.name.toLowerCase();
  
  console.log(`\n======================================================`);
  console.log(`[SERVER] STARTING IMPORT PIPELINE: ${fileObj.name}`);
  console.log(`======================================================`);
  
  let headerRow = 1; 
  let dataRow = 2;
  let forcedDelimiter = null; 
  let specialOptions = getSpecialFileOptions_(lowerName) || {};
  
  for (let i = 0; i < FILE_RULES.length; i++) {
    if (lowerName.includes(FILE_RULES[i].keyword)) {
      headerRow = FILE_RULES[i].headerRow;
      dataRow = FILE_RULES[i].dataRow;
      forcedDelimiter = FILE_RULES[i].delimiter || null;
      console.log(`[SERVER] Match found! Rule: ${FILE_RULES[i].keyword}. Header: ${headerRow}, Data: ${dataRow}`);
      break;
    }
  }

  if (specialOptions.headerRow) headerRow = specialOptions.headerRow;
  if (specialOptions.dataRow) dataRow = specialOptions.dataRow;

  // Chunked CSV files are already normalized: header row 1, data row 2, comma-delimited.
  let isChunked = !!(fileObj.parts && fileObj.parts.length > 0);
  if (isChunked) {
    headerRow = 1;
    dataRow = 2;
    forcedDelimiter = null;
    console.log(`[SERVER] Chunked import detected (${fileObj.parts.length} parts). Using normalized CSV defaults.`);
  }

  let tableName = cleanTableName(fileObj.name);
  let tempTableId = tableName + '_temp_ext'; 
  console.log(`[SERVER] Target Table: ${tableName}`);
  
  try {
    console.log(`[SERVER] Cleaning up old temp tables...`);
    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch (e) { }

    let schemaData = buildDynamicSchema(fileObj.id, headerRow, dataRow, forcedDelimiter, GCP_PROJECT_ID, DATASET_ID, fileObj.mimeType, specialOptions);
    let finalSchema = schemaData.schema;
    let fileDelimiter = schemaData.delimiter;

    console.log(`[SERVER] Creating Ghost Table...`);
    let ghostSchemaFields = finalSchema.map(f => ({ name: f.name, type: 'STRING' }));
    let isSheet = fileObj.mimeType === MimeType.GOOGLE_SHEETS;

    // Build source URIs – chunked imports pass all parts so BQ reads them as one table.
    let sourceUris;
    if (isChunked) {
      sourceUris = fileObj.parts.map(function(p) { return 'https://drive.google.com/open?id=' + p.id; });
    } else if (isSheet) {
      sourceUris = [`https://docs.google.com/spreadsheets/d/${fileObj.id}`];
    } else {
      sourceUris = [`https://drive.google.com/open?id=${fileObj.id}`];
    }

    let externalDataConfiguration = {
      sourceUris: sourceUris,
      sourceFormat: isSheet ? "GOOGLE_SHEETS" : "CSV",
      autodetect: false
    };

    if (specialOptions.keepColumnIndexes && specialOptions.keepColumnIndexes.length) {
      externalDataConfiguration.ignoreUnknownValues = true;
    }

    if (isSheet) {
      externalDataConfiguration.googleSheetsOptions = { skipLeadingRows: dataRow - 1 };
      let limitedRange = buildSheetRangeFromColumnIndexes_(specialOptions.keepColumnIndexes || []);
      if (limitedRange) externalDataConfiguration.googleSheetsOptions.range = limitedRange;
    } else {
      externalDataConfiguration.csvOptions = { skipLeadingRows: dataRow - 1, allowQuotedNewlines: true, fieldDelimiter: fileDelimiter };
    }

    let tableResource = {
      tableReference: { projectId: GCP_PROJECT_ID, datasetId: DATASET_ID, tableId: tempTableId },
      schema: { fields: ghostSchemaFields }, 
      externalDataConfiguration: externalDataConfiguration
    };
    
    BigQuery.Tables.insert(tableResource, GCP_PROJECT_ID, DATASET_ID);

    console.log(`[SERVER] Compiling dynamic SAFE_CAST SQL...`);
    let selectCols = finalSchema.map(f => {
      let colName = `\`${f.name}\``;
      let cleanStr = `CASE WHEN LOWER(TRIM(${colName})) IN ('', 'null', '-') THEN NULL ELSE TRIM(${colName}) END`;
      let sqlType = f.type.toUpperCase();

      if (sqlType === 'BIGNUMERIC') {
        let noCurrency = `REGEXP_REPLACE(${cleanStr}, r'[^0-9,.-]', '')`;
        if (fileDelimiter === ';') {
          return `SAFE_CAST(REPLACE(REPLACE(${noCurrency}, '.', ''), ',', '.') AS BIGNUMERIC) AS ${colName}`;
        } else {
          return `SAFE_CAST(REPLACE(${noCurrency}, ',', '') AS BIGNUMERIC) AS ${colName}`;
        }
      }
      else if (sqlType === 'NUMERIC') {
        let noCurrency = `REGEXP_REPLACE(${cleanStr}, r'[^0-9,.-]', '')`;
        if (fileDelimiter === ';') {
          return `SAFE_CAST(REPLACE(REPLACE(${noCurrency}, '.', ''), ',', '.') AS NUMERIC) AS ${colName}`;
        } else {
          return `SAFE_CAST(REPLACE(${noCurrency}, ',', '') AS NUMERIC) AS ${colName}`;
        }
      } 
      else if (sqlType === 'INT64') {
        let noCurrency = `REGEXP_REPLACE(${cleanStr}, r'[^0-9,.-]', '')`;
        if (fileDelimiter === ';') {
          return `SAFE_CAST(REPLACE(REPLACE(${noCurrency}, '.', ''), ',', '') AS INT64) AS ${colName}`;
        } else {
          return `SAFE_CAST(SAFE_CAST(REPLACE(${noCurrency}, ',', '') AS NUMERIC) AS INT64) AS ${colName}`;
        }
      } 
      else if (sqlType === 'DATE') {
        // Support multiple date layouts: ISO, dd/MM/yyyy, MM/dd/yyyy, dd.MM.yyyy,
        // MM-dd-yyyy and 2-digit year variants while staying safe for malformed values.
        return `
          COALESCE(
            SAFE_CAST(SUBSTR(${cleanStr}, 1, 10) AS DATE),
            SAFE.PARSE_DATE('%Y/%m/%d', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}')),
            SAFE.PARSE_DATE('%Y.%m.%d', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{4}\\.[0-9]{1,2}\\.[0-9]{1,2}')),
            SAFE.PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}')),
            SAFE.PARSE_DATE('%d/%m/%Y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')),
            SAFE.PARSE_DATE('%m/%d/%Y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')),
            SAFE.PARSE_DATE('%d.%m.%Y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{4}')),
            SAFE.PARSE_DATE('%m.%d.%Y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{4}')),
            SAFE.PARSE_DATE('%d-%m-%Y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}')),
            SAFE.PARSE_DATE('%m-%d-%Y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}')),
            SAFE.PARSE_DATE('%d/%m/%y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}')),
            SAFE.PARSE_DATE('%m/%d/%y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}')),
            SAFE.PARSE_DATE('%d.%m.%y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{2}')),
            SAFE.PARSE_DATE('%m.%d.%y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{2}')),
            SAFE.PARSE_DATE('%d-%m-%y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}-[0-9]{1,2}-[0-9]{2}')),
            SAFE.PARSE_DATE('%m-%d-%y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}-[0-9]{1,2}-[0-9]{2}'))
          ) AS ${colName}
        `.trim();
      }
      else if (sqlType === 'BOOL') {
        return `
          CASE
            WHEN LOWER(TRIM(${colName})) IN ('true', '1', 'yes', 'ja') THEN TRUE
            WHEN LOWER(TRIM(${colName})) IN ('false', '0', 'no', 'nein') THEN FALSE
            ELSE NULL
          END AS ${colName}
        `.trim();
      } 
      else {
        return `${cleanStr} AS ${colName}`;
      }
    }).join(',\n          ');

    let insertHeaders = finalSchema.map(f => `\`${f.name}\``).join(', ');

    let query = `
      CREATE OR REPLACE TABLE \`${GCP_PROJECT_ID}.${DATASET_ID}.${tableName}\` (
        ${finalSchema.map(f => `\`${f.name}\` ${f.type}`).join(', ')}
      );
      
      INSERT INTO \`${GCP_PROJECT_ID}.${DATASET_ID}.${tableName}\` (${insertHeaders})
      SELECT ${selectCols} FROM \`${GCP_PROJECT_ID}.${DATASET_ID}.${tempTableId}\`;
    `;

    console.log(`[SERVER] Sending SQL job to BigQuery...`);
    let queryJobConfig = { configuration: { query: { query: query, useLegacySql: false } } };
    let insertedJob = BigQuery.Jobs.insert(queryJobConfig, GCP_PROJECT_ID);
    let jobId = insertedJob.jobReference.jobId;
    let jobLocation = insertedJob.jobReference.location; 
    
    console.log(`[SERVER] Job ID ${jobId} successfully submitted. Beginning polling loop...`);
    
    let maxAttempts = 150; 
    let success = false;
    let errorMsg = "";

    for (let i = 0; i < maxAttempts; i++) {
      try {
        let job = BigQuery.Jobs.get(GCP_PROJECT_ID, jobId, { location: jobLocation });
        console.log(`   -> Polling [${i+1}/${maxAttempts}]: State is ${job.status.state}`);
        
        if (job.status.state === 'DONE') {
          if (job.status.errorResult) errorMsg = job.status.errorResult.message;
          else success = true;
          break;
        }
      } catch (pollError) {
        console.log(`   -> Minor API hiccup (${pollError.message}). Retrying...`);
      }
      Utilities.sleep(2000); 
    }

    console.log(`[SERVER] Cleaning up Ghost Table...`);
    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch(e) {}
    
    if (success) {
      console.log(`[SUCCESS] BigQuery import successful. Moving file(s) to Archive...`);
      if (isChunked) {
        fileObj.parts.forEach(function(p) { DriveApp.getFileById(p.id).moveTo(archiveFolder); });
      } else {
        file.moveTo(archiveFolder);
      }
      return { success: true, log: `[SUCCESS] Injected into '${tableName}'. Moved to Archive.` };
    } else if (errorMsg) {
      console.error(`[CRASH] BigQuery rejected the file: ${errorMsg}`);
      return { success: false, log: `[ERROR] BigQuery rejected ${fileObj.name}: ${errorMsg}` };
    } else {
      console.error(`[CRASH] Polling timed out after 5 minutes.`);
      return { success: false, log: `[ERROR] Timeout waiting for database.` };
    }
    
  } catch (error) {
    console.error(`[CRASH] Critical Pipeline Error: ${error.message}`);
    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch(e){}
    return { success: false, log: `[CRITICAL] Connection failed: ${error.message}` };
  }
}