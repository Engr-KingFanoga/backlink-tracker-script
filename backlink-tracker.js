/**
 * This function is intended to be run by a monthly trigger.
 * It resets the currentRow counter and installs a minute-based trigger
 * to process the backlink checks in batches.
 **/
function startMonthlyBatchProcessing() {
  // Reset the progress counter (starting at row 2, assuming row 1 is header)
  var scriptProperties = PropertiesService.getScriptProperties();
  scriptProperties.setProperty("currentRow", "2");
  scriptProperties.setProperty("currentSheetIndex", "0");

  // List of sheet names to process
  var sheetsToProcess = ["RAW DATA", "2025 RAW", "2026 RAW"];
  scriptProperties.setProperty("sheetsToProcess", JSON.stringify(sheetsToProcess));
  
  // Create a minute trigger to run processBacklinkBatch() every 5 minute.
  ScriptApp.newTrigger("processBacklinkBatch")
           .timeBased()
           .everyMinutes(5)
           .create();
  Logger.log("Monthly processing started for multiple sheets.");
}

/**
 * This function is executed every minute by a time-driven trigger.
 * It processes a batch of rows and then updates the progress counter.
 * When all rows in all sheets have been processed, it deletes its own trigger.
 **/
function processBacklinkBatch() {
  var scriptProperties = PropertiesService.getScriptProperties();

  // Retrieve the current row and current sheet index from script properties
  var currentRow = parseInt(scriptProperties.getProperty("currentRow"), 10);
  var currentSheetIndex = parseInt(scriptProperties.getProperty("currentSheetIndex"), 10);
  
  // Retrieve the list of sheets to process (stored as JSON string)
  var sheetsToProcess = JSON.parse(scriptProperties.getProperty("sheetsToProcess"));

  // If all sheets have been processed, stop the trigger
  if (currentSheetIndex >= sheetsToProcess.length) {
    Logger.log("All sheets processed. Deleting trigger.");
    deleteProcessBacklinkBatchTriggers();
    return;
  }

  // Get the current sheet by name
  var sheetName = sheetsToProcess[currentSheetIndex];
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(sheetName);

  if (!sheet) {
    Logger.log("Sheet not found: " + sheetName);
    // Move on to the next sheet
    scriptProperties.setProperty("currentSheetIndex", (currentSheetIndex + 1).toString());
    scriptProperties.setProperty("currentRow", "2");
    return;
  }

  // To avoid running the script on empty rows(when rows to be processed is smaller than batch size), count only rows with a URL in column D.
  var urlData = sheet.getRange("D:D").getValues();
  // Assuming row 1 is header; count nonempty rows from row 2 onward.
  var inputRows = urlData.slice(1).filter(function(row) {
    return row[0] && row[0].toString().trim() !== "";
  }).length;

  // Use inputRows + 1 (to account for the header) as your last row.
  var lastRow = inputRows + 1;

  // Define the batch size (e.g., 250 rows per execution).
  var batchSize = 250;
  var endRow = currentRow + batchSize - 1;

  // Make sure we do not exceed the last row.
  if (endRow > lastRow) {
    endRow = lastRow;
  }

  Logger.log("Processing " + sheetName + " rows " + currentRow + " to " + endRow);

  // Process this batch.
  checkBacklinksForBatch(sheetName, currentRow, endRow);

  // If we've processed all rows in the current sheet, move to the next one
  if (endRow >= lastRow) {
    scriptProperties.setProperty("currentSheetIndex", (currentSheetIndex + 1).toString());
    scriptProperties.setProperty("currentRow", "2");
  } else {
    // Otherwise, continue processing the same sheet in the next run
    scriptProperties.setProperty("currentRow", (endRow + 1).toString());
  }
}

/**
 * This function processes a batch of rows from startRow to endRow.
 * It checks each row for:
 *   - Whether the website URL (Column A) and VEED URL (Column B) are accessible.
 *   - Whether the website page contains an anchor linking to the VEED URL.
 *
 * The results are written in:
 *   - Column C ("status"): "live" or "missing"
 *   - Column D ("time checked"): current timestamp (or "missing" if not found)
 *   - Column E ("remarks"): additional info such as "nofollow link" or error details.
 *
 * If a backlink is missing or errors occur, you might want to add the row to an email queue.
 */
function checkBacklinksForBatch(sheetName, startRow, endRow) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(sheetName);
  // Get all data from the sheet.
  var data = sheet.getDataRange().getValues();
  var currentTime = new Date();
  
  // Loop over the specified rows.
  // Note: data array is 0-indexed; row 1 (header) is at index 0.
  for (var i = startRow - 1; i < endRow; i++) {
    var websiteUrl = data[i][3]; // Column D
    var veedUrl = data[i][9];    // Column J

  // Skip processing if websiteUrl is empty ---
    if (!websiteUrl || websiteUrl.toString().trim() === "") {
      Logger.log("Skipping row " + (i + 1) + " because websiteUrl is empty.");
      continue;
    }

    var status = "";
    var remark = "";
    
    try {
      // Fetch the website URL.
      var websiteResponse = UrlFetchApp.fetch(websiteUrl, {muteHttpExceptions: true, followRedirects: true});
      if (websiteResponse.getResponseCode() !== 200) {
        if (websiteResponse.getResponseCode() === 403) {
          status = "unknown";
          remark = "Website fetch error: 403 (website forbidden)";
        } 
        else if (websiteResponse.getResponseCode() === 401) {
          status = "unknown";
          remark = "Website fetch error: 401 (unauthorized)";
        }
        else {
          status = "missing";
          // Check for specific error codes and set remarks accordingly.
          if (websiteResponse.getResponseCode() === 202) {
            remark = "Website fetch error: 202 (accepted (but unexpected error))";
          }
          else if (websiteResponse.getResponseCode() === 400) {
            remark = "Website fetch error: 400 (bad request)";
          }
          else if (websiteResponse.getResponseCode() === 404) {
            remark = "Website fetch error: 404 (page not found)";
          }
          else if (websiteResponse.getResponseCode() === 408) {
            remark = "Website fetch error: 408 (request timeout)";
          }
          else if (websiteResponse.getResponseCode() === 429) {
            remark = "Website fetch error: 429 (too many requests)";
          }
          else if (websiteResponse.getResponseCode() === 500) {
            remark = "Website fetch error: 500 (internal server error)";
          }
          else if (websiteResponse.getResponseCode() === 502) {
            remark = "Website fetch error: 502 (bad gateway)";
          }
          else if (websiteResponse.getResponseCode() === 503) {
            remark = "Website fetch error: 503 (service unavailable)";
          }
          else if (websiteResponse.getResponseCode() === 504) {
            remark = "Website fetch error: 504 (gateway timeout)";
          }
          else if (websiteResponse.getResponseCode() === 520) {
            remark = "Website fetch error: 520 (Cloudflare unknown error)";
          }
          else if (websiteResponse.getResponseCode() === 522) {
            remark = "Website fetch error: 522 (Cloudflare connection timed out)";
          }
          else {
            remark = "Website fetch error: " + websiteResponse.getResponseCode() + " (unexpected error)";
          }
        }
      } else {
        // Fetch the VEED URL.
        var veedResponse = UrlFetchApp.fetch(veedUrl, {muteHttpExceptions: true, followRedirects: true});
        if (veedResponse.getResponseCode() !== 200) {
          if (veedResponse.getResponseCode() === 400) {
            // VEED fetch failed, but still try to look for any VEED links on the website
            var websiteContent = websiteResponse.getContentText();
            var veedPattern = /https?:\/\/(www\.)?veed\.io\/[^\s"'<>]*/gi;
            var veedMatches = websiteContent.match(veedPattern);
            if (veedMatches && veedMatches.length > 0) {
              status = "live";
              remark = "VEED fetch error: 400, but other VEED link(s) found: " + veedMatches.join(", ");
            } else {
              status = "missing";
              remark = "VEED fetch error: 400 (no VEED links found on site)";
            }
          } else {
            status = "missing";
            remark = "VEED fetch error: " + veedResponse.getResponseCode();
          }       
        } else {
          // Both pages are reachable. Now check if the website content contains the VEED URL.
          var websiteContent = websiteResponse.getContentText();
          // Create an array with both the HTTPS and HTTP versions.
          var versions = [veedUrl, veedUrl.replace("https://", "http://")];
          var found = false;
          
          for (var j = 0; j < versions.length; j++) {
            var version = versions[j];
            var escapedVersion = version.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
            var regex = new RegExp('<a\\s[^>]*href=["\']' + escapedVersion + '["\'][^>]*>', 'i');
            var match = websiteContent.match(regex);
            if (match) {
              // Determine if "nofollow" exists in the anchor tag.
              if (/rel\s*=\s*["'][^"']*nofollow[^"']*["']/i.test(match[0])) {
                status = "live";
                remark = (j === 1) ? "nofollow link (http version)" : "nofollow link";
              } else {
                status = "live";
                remark = (j === 1) ? "http version found" : "";
              }
              found = true;
              break;
            }
          }
          
          // If not found, check for any VEED.io link
          if (!found) {
            var veedPattern = /https?:\/\/(www\.)?veed\.io\/[^\s"'<>]*/gi;
            var veedMatches = websiteContent.match(veedPattern);
            if (veedMatches && veedMatches.length > 0) {
              status = "live";
              remark = "Different VEED link(s) found: " + veedMatches.join(", ");
              found = true;
            }
          }
          
          // If still not found, then VEED backlink is missing
          if (!found) {
            status = "missing";
            remark = "";
          }
        }
      }
    } catch (e) {
      var errorMessage = e.toString().toLowerCase();
      
      // Check for common SSL-related phrases
      if (errorMessage.includes("ssl") || 
          errorMessage.includes("certificate") || 
          errorMessage.includes("handshake") || 
          errorMessage.includes("secure connection")) {
        status = "unknown";
      } else {
        status = "missing";
      }
      remark = e.toString();
    }
    
    // Update the sheet for this row.
    var statusCell = sheet.getRange(i + 1, 24);  // Column X: Status
    statusCell.setValue(status);

    var timeCell = sheet.getRange(i + 1, 25);  // Column Y: Time checked
    timeCell.setValue(currentTime);

    var remarkCell = sheet.getRange(i + 1, 26);  // Column Z: Remarks
    remarkCell.setValue(remark);

    // Apply color coding based on status
    if (status === "live") {
      statusCell.setBackground("#1BB544");  // Dark Pastel Green
      if (remark === "") {
        remarkCell.setBackground("#FFFFFF");  // White
      } else if (remark === "nofollow link") {
        remarkCell.setBackground("#1C9AB6");  // Blue Green
      } else if (remark === "http version found") {
        remarkCell.setBackground("#1BB559");  // Pigment Green
      } else if (remark === "nofollow link (http version)") {
        remarkCell.setBackground("#1C9AB6");  // Blue Green
      } else if (remark.includes("Different VEED link(s) found:") || remark.includes("VEED fetch error: 400, but other VEED link(s) found:"))
        remarkCell.setBackground("#1BB58C"); // Mint
    } else if (status === "missing") {
      statusCell.setBackground("#C63A3A");  // Persian Red
      if (remark === "") {
        remarkCell.setBackground("#FFFFFF");  // White
      } else if (remark.includes("Website fetch error:") || remark.includes("VEED fetch error:")) {
        remarkCell.setBackground("#C75F3A");  // Flame
      } else {
        remarkCell.setBackground("#C7723A");  // Cocoa brown (default for other missing cases)
      }
    } else if (status === "unknown") {
      statusCell.setBackground("#F7B32B");  // Xanthous
      remarkCell.setBackground("#E6E6FA");  // Lavender
    }
    
    // Optionally, if a row is marked "missing", add it to an email queue.
    if (status === "missing") {
      addToEmailQueue(websiteUrl, veedUrl, currentTime, remark);
    }
  }
} 

/**
 * Deletes all triggers that call the processBacklinkBatch function.
 * This is used to stop the minute-based trigger once all batches have been processed.
 */

function deleteProcessBacklinkBatchTriggers() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === "processBacklinkBatch") {
      ScriptApp.deleteTrigger(triggers[i]);
      Logger.log("Deleted trigger: " + triggers[i].getUniqueId());
    }
  }
}

/**
 * (Optional) Helper function to add a missing backlink record to an email queue.
 * You can implement this as needed (for example, appending to a sheet named "EmailQueue").
 */
function addToEmailQueue(websiteUrl, veedUrl, timeChecked, remark) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var queueSheet = ss.getSheetByName("EmailQueue");
  if (!queueSheet) {
    // Create the EmailQueue sheet if it does not exist.
    queueSheet = ss.insertSheet("EmailQueue");
    queueSheet.appendRow(["Website URL", "VEED URL", "Time Checked", "Remark"]);
  }
  queueSheet.appendRow([websiteUrl, veedUrl, timeChecked, remark]);
}