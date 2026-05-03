const WebSocket = require('ws');
const ws = new WebSocket('wss://uzakmasaustu-relay.onrender.com/ws');

ws.on('open', () => {
    console.log('Connected to relay');
    ws.send(JSON.stringify({
        type: 'Register',
        id: '999-999-999',
        hostname: 'TestClient'
    }));
});

ws.on('message', (data) => {
    const txt = data.toString();
    console.log('Got msg size:', txt.length, 'preview:', txt.substring(0, 100));
    
    try {
        const json = JSON.parse(txt);
        if (json.type === 'Registered') {
            console.log('Registered! Now connecting to host 612-319-316...');
            ws.send(JSON.stringify({
                type: 'ConnectTo',
                target_id: '612-319-316',
                hostname: 'TestClient'
            }));
        } else if (json.type === 'Paired') {
            console.log('PAIRED! Waiting for frames...');
        }
    } catch(e) {
        // It's probably an encrypted frame
    }
});

ws.on('close', () => console.log('Closed'));
ws.on('error', (e) => console.log('Error', e));
