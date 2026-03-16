export function parseFirebaseConfig(pastedConfig: string): any {
    if (!pastedConfig) return null;

    try {
        const match = pastedConfig.match(/const\s+\w+\s*=\s*({[\s\S]*?});/);
        let jsonStr = match ? match[1] : pastedConfig;

        jsonStr = jsonStr
            .replace(/([{,]\s*)([a-zA-Z0-9_]+)\s*:/g, '$1"$2":')
            .replace(/:\s*'([^']*)'/g, ':"$1"')
            .replace(/,(\s*[}\]])/g, '$1');

        return JSON.parse(jsonStr);
    } catch (e) {
        throw new Error("Failed to parse configuration. Please ensure it is valid JSON or a valid JavaScript object literal.");
    }
}
