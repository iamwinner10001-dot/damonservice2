/**
 * DAMON SERVICE - GOOGLE SHEETS BACKEND
 * 
 * Instructions:
 * 1. Create a new Google Sheet.
 * 2. Create 6 Tabs named exactly: "Users", "Projects", "Inquiries", "Devices", "Categories", "Comments".
 * 3. Go to Extensions > Apps Script.
 * 4. Paste this code completely.
 * 5. Click "Deploy" > "New deployment".
 * 6. Select type: "Web app".
 * 7. Description: "v1".
 * 8. Execute as: "Me".
 * 9. Who has access: "Anyone" (Important for the app to work without login popup).
 * 10. Copy the Web App URL and paste it into the Admin Settings of your app.
 */

function doGet() {
  const wb = SpreadsheetApp.getActiveSpreadsheet();
  
  const data = {
    users: getSheetData(wb, 'Users'),
    projects: getSheetData(wb, 'Projects'),
    inquiries: getSheetData(wb, 'Inquiries'),
    devices: getSheetData(wb, 'Devices'),
    categories: getSheetData(wb, 'Categories'),
    comments: getSheetData(wb, 'Comments')
  };
  
  return ContentService.createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  const wb = SpreadsheetApp.getActiveSpreadsheet();
  const postData = JSON.parse(e.postData.contents);
  const action = postData.action;
  const payload = postData.payload;
  
  let sheetName = '';
  
  // Map actions to sheets
  if (action.includes('user')) sheetName = 'Users';
  else if (action.includes('project')) sheetName = 'Projects';
  else if (action.includes('inquiry')) sheetName = 'Inquiries';
  else if (action.includes('device')) sheetName = 'Devices';
  else if (action.includes('category')) sheetName = 'Categories';
  else if (action.includes('comment')) sheetName = 'Comments';
  
  if (!sheetName) {
    return ContentService.createTextOutput(JSON.stringify({status: 'error', message: 'Unknown action'}));
  }
  
  const sheet = wb.getSheetByName(sheetName);
  
  if (action.startsWith('create_')) {
    // Basic Append Row Logic
    // Note: For production, you should align columns strictly. 
    // Here we dump the JSON object as a string in one column or map fields dynamically.
    // For this prototype, we will try to map common fields or just append the JSON for simplicity if schema varies too much.
    // BETTER APPROACH: Map object values to array based on headers.
    
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn() || 1).getValues()[0];
    if (headers.length === 0 || (headers.length === 1 && headers[0] === '')) {
      // Initialize headers if empty
      const keys = Object.keys(payload);
      sheet.appendRow(keys);
    }
    
    // Re-read headers
    const currentHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const row = currentHeaders.map(header => {
      let val = payload[header];
      if (typeof val === 'object') return JSON.stringify(val);
      return val;
    });
    
    sheet.appendRow(row);
    
  } else if (action.startsWith('update_')) {
    // Find row by ID and update
    const data = sheet.getDataRange().getValues();
    const headers = data[0];
    const idIndex = headers.indexOf('id');
    
    if (idIndex === -1) return; // No ID column found
    
    for (let i = 1; i < data.length; i++) {
      if (data[i][idIndex] == payload.id) {
        // Found row, update fields
        Object.keys(payload).forEach(key => {
          const colIndex = headers.indexOf(key);
          if (colIndex > -1) {
            let val = payload[key];
            if (typeof val === 'object') val = JSON.stringify(val);
            sheet.getRange(i + 1, colIndex + 1).setValue(val);
          }
        });
        break;
      }
    }
  } else if (action.startsWith('delete_')) {
    // Find and delete
    const data = sheet.getDataRange().getValues();
    const headers = data[0];
    const idIndex = headers.indexOf('id');
    
    for (let i = 1; i < data.length; i++) {
      if (data[i][idIndex] == payload.id) {
        sheet.deleteRow(i + 1);
        break;
      }
    }
  }
  
  return ContentService.createTextOutput(JSON.stringify({status: 'success'}));
}

function getSheetData(wb, sheetName) {
  const sheet = wb.getSheetByName(sheetName);
  if (!sheet) return [];
  
  const rows = sheet.getDataRange().getValues();
  if (rows.length < 2) return []; // Only headers or empty
  
  const headers = rows[0];
  const data = [];
  
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const obj = {};
    let hasData = false;
    
    headers.forEach((header, index) => {
      let val = row[index];
      // Try to parse JSON strings back to objects (for nested data like breakdown)
      if (typeof val === 'string' && (val.startsWith('{') || val.startsWith('['))) {
        try { val = JSON.parse(val); } catch(e) {}
      }
      obj[header] = val;
      if (val !== '') hasData = true;
    });
    
    if (hasData) data.push(obj);
  }
  
  return data;
}
