// Use CommonJS require for n8n Code node compatibility
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const { DateTime } = require('luxon');

// Supabase config - In n8n, these should ideally come from credentials or node parameters
const SUPABASE_URL = 'https://lejpfxtpefbplqaxczfx.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxlanBmeHRwZWZicGxxYXhjemZ4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0MzUxNzYzNCwiZXhwIjoyMDU5MDkzNjM0fQ.XpxXMXdY1mH1ZxrzOYDxttc9SZHbfRAvOgmuGZUgWpU';

// CDR API credentials - In n8n, this should ideally come from credentials
const CDR_TOKEN = '11d76ffd-8618-45ff-8a67-afff50e1e98f';

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Helper to format Date to YYYY-MM-DD (UTC)
function formatDateUTC(date) {
  const pad = n => String(n).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}`;
}

async function fetchCdrDataAndStore() {
  const scriptStartTime = Date.now();
  let logEntryId = null;
  let status = 'started';
  let message = 'Script execution started.';
  let recordsFetched = null;
  let recordsDeduplicated = null;
  let recordsAdded = null;
  let initialDbCount = null;
  let finalDbCount = null;
  let executionSummary = {};

  try {
    // Insert initial log entry
    const { data: logData, error: logError } = await supabase
      .from('script_logs')
      .insert([{
        run_timestamp: new Date(scriptStartTime).toISOString(),
        script_name: 'n8n_fetch_cdr_dump',
        status: status,
        message: 'Initial log entry from n8n script.'
      }])
      .select('id')
      .single();

    if (logError) {
      console.error('Failed to insert initial script log:', logError);
    } else if (logData) {
      logEntryId = logData.id;
    }

    const today = new Date();
    const endDateForApi = formatDateUTC(today);
    const startDateObj = new Date(today);
    startDateObj.setDate(today.getDate() - 7);
    const startDateForApi = formatDateUTC(startDateObj);

    const start = startDateForApi;
    const end = endDateForApi;

    console.log(`Fetching CDR data from ${start} to ${end}...`);

    const { count: initialCount, error: initialCountError } = await supabase
      .from('cdr_logs')
      .select('*', { count: 'exact', head: true });

    if (initialCountError) {
      console.error('Initial row-count query error:', initialCountError);
    } else {
      initialDbCount = initialCount;
      console.log(`Initial rows in cdr_logs: ${initialDbCount}`);
    }

    const resp = await axios.get(
      'https://jegantic.nx0.ca/capi/query.php',
      {
        headers: { Authorization: `Bearer ${CDR_TOKEN}` },
        params: { endpoint: 'cdr', start, end },
        responseType: 'json'
      }
    );

    let rawRecords = Array.isArray(resp.data) ? resp.data : JSON.parse(resp.data);
    recordsFetched = rawRecords.length;
    console.log(`Received ${recordsFetched} records from API.`);

    const uniqueRecordsMap = new Map();
    rawRecords.forEach(rec => uniqueRecordsMap.set(rec.uniqueid, rec));
    let records = Array.from(uniqueRecordsMap.values());
    recordsDeduplicated = records.length;

    if (recordsDeduplicated < recordsFetched) {
      console.log(`Deduplicated records: reduced from ${recordsFetched} to ${recordsDeduplicated}`);
    }
    console.log(`Processing ${recordsDeduplicated} unique records.`);

    const batchSize = 100;
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize).map(rec => {
        const isoCalldate = rec.calldate.replace(' ', 'T') + 'Z';
        const utc = DateTime.fromISO(isoCalldate, { zone: 'utc' });
        const local = utc.setZone('America/Toronto');
        const calldateLocalESTString = local.toFormat('yyyy-MM-dd HH:mm:ss');

        return {
          did: rec.did,
          uniqueid: rec.uniqueid,
          linkedid: rec.linkedid,
          calldate: rec.calldate,
          calldate_local: calldateLocalESTString,
          clid: rec.clid,
          src: rec.src,
          cnam: rec.cnam,
          dst: rec.dst,
          dst_cnam: rec.dst_cnam,
          duration: parseInt(rec.duration, 10),
          billsec: parseInt(rec.billsec, 10),
          channel: rec.channel,
          lastapp: rec.lastapp,
          amaflags: rec.amaflags,
          dcontext: rec.dcontext,
          lastdata: rec.lastdata,
          sequence: rec.sequence,
          userfield: rec.userfield,
          dstchannel: rec.dstchannel,
          accountcode: rec.accountcode,
          disposition: rec.disposition,
          peeraccount: rec.peeraccount,
          outbound_cnam: rec.outbound_cnam,
          outbound_cnum: rec.outbound_cnum,
          recordingfile: rec.recordingfile,
          raw: rec
        };
      });

      const { error: upsertError } = await supabase
        .from('cdr_logs')
        .upsert(batch, { onConflict: ['uniqueid'] });

      if (upsertError) {
        console.error(`Batch upsert error:`, upsertError);
        throw new Error(`Batch upsert failed: ${upsertError.message}`);
      }
    }

    const { count: finalCount, error: countError } = await supabase
      .from('cdr_logs')
      .select('*', { count: 'exact', head: true });

    if (countError) {
      console.error('Final row-count query error:', countError);
    } else {
      finalDbCount = finalCount;
      console.log(`Total rows in cdr_logs after import: ${finalDbCount}`);
      if (initialDbCount !== null && finalDbCount !== null) {
        recordsAdded = finalDbCount - initialDbCount;
        console.log(`Added ${recordsAdded} new records.`);
      }
    }
    status = 'completed';
    message = `CDR import process complete. Fetched: ${recordsFetched}, Deduplicated: ${recordsDeduplicated}, Added: ${recordsAdded}.`;
    console.log(message);

  } catch (error) {
    console.error('Unhandled error during script execution:', error);
    status = 'failed';
    message = error.message || 'Unknown error during script execution.';
  } finally {
    const durationMs = Date.now() - scriptStartTime;
    const logPayload = {
      status: status,
      message: message,
      records_fetched: recordsFetched,
      records_deduplicated: recordsDeduplicated,
      records_added: recordsAdded,
      duration_ms: durationMs,
      initial_db_count: initialDbCount,
      final_db_count: finalDbCount
    };

    if (logEntryId) {
      const { error: updateLogError } = await supabase
        .from('script_logs')
        .update(logPayload)
        .eq('id', logEntryId);
      if (updateLogError) console.error('Failed to update script log:', updateLogError);
    } else {
      const { error: insertLogError } = await supabase
        .from('script_logs')
        .insert([{
          run_timestamp: new Date(scriptStartTime).toISOString(),
          script_name: 'n8n_fetch_cdr_dump',
          ...logPayload
        }]);
      if (insertLogError) console.error('Failed to insert final script log:', insertLogError);
    }
    console.log(`Script finished. Status: ${status}, Duration: ${durationMs}ms`);
    executionSummary = {
        status,
        message,
        recordsFetched,
        recordsDeduplicated,
        recordsAdded,
        durationMs,
        logEntryId
    };
  }
  return [executionSummary]; // n8n expects an array of items
}

// When pasting into n8n's Code node, the node itself often wraps this
// in an async function and executes it.
// The last expression evaluated is typically returned.
// So, we call the function here.
// module.exports = { execute: fetchCdrDataAndStore }; // This is for when n8n requires a file with an execute method.
// For direct pasting, the below is more common:
return fetchCdrDataAndStore();