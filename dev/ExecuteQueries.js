// ============================================================================
// EXECUTE STORED PROCEDURES PIPELINE
// ============================================================================
const EQ_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b';
const EQ_DATASET_ID = 'staging';

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
    name: 'sp_build_aktionsplan_int_pq',
    label: 'Aktionsplan INT PQ',
    call: `CALL \`${EQ_PROJECT_ID}.${EQ_DATASET_ID}.sp_aktionsplan_int_pq\`()`
  }
];

// ============================================================================
// UI TRIGGER
// ============================================================================
function openExecuteQueriesUI() {
  const html = HtmlService.createHtmlOutputFromFile('ExecuteQueriesUI')
    .setWidth(700)
    .setHeight(560)
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
// GET TOTAL PROCEDURE COUNT (for UI)
// ============================================================================
function getStoredProcedureCount() {
  return EQ_PROCEDURES.length;
}
