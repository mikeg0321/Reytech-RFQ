/**
 * Background Service Worker — relays prices from S&S tab to Reytech tab
 */
chrome.runtime.onMessage.addListener(function(msg, sender, sendResponse) {
  if (msg.type === 'reytech_ssww_prices') {
    console.log('[BG] Got prices from S&S:', msg);
    // Forward to ALL Reytech tabs
    chrome.tabs.query({url: 'https://web-production-dcee9.up.railway.app/*'}, function(tabs) {
      tabs.forEach(function(tab) {
        chrome.tabs.sendMessage(tab.id, msg);
        console.log('[BG] Forwarded to Reytech tab', tab.id);
      });
    });
    // Close the S&S tab after a delay
    if (sender.tab) {
      setTimeout(function() { chrome.tabs.remove(sender.tab.id); }, 2000);
    }
    sendResponse({ok: true});
  }
  return true;
});
