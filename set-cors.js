// removed unused firebase imports

async function setCors() {
    const url = `http://127.0.0.1:9199/v1/b/demo-y-cinder.appspot.com`;
    console.log("Setting CORS on", url);

    try {
        const response = await fetch(url, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer owner'
            },
            body: JSON.stringify({
                cors: [{
                    origin: ['*'],
                    method: ['GET', 'PUT', 'POST', 'DELETE', 'OPTIONS'],
                    responseHeader: ['*'],
                    maxAgeSeconds: 3600
                }]
            })
        });
        console.log(response.status, await response.text());
    } catch (e) {
        console.error(e);
    }
}
setCors();
