export async function waitForCondition(
    predicate: () => boolean | Promise<boolean>,
    timeout: number = 2000,
    interval: number = 50,
    message: string = 'Condition not met'
): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeout) {
        if (await predicate()) {
            return;
        }
        await new Promise(r => setTimeout(r, interval));
    }
    throw new Error(`${message} (timed out after ${timeout}ms)`);
}
