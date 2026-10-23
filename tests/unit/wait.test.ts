import { describe, it, expect } from 'vitest';
import {
    waitForCondition,
    waitForConditionEquals,
    waitForConditionGreaterThan,
    waitForConditionLessThan,
    waitForConditionNotEquals,
    waitForConditionTruthy
} from '../utils/wait';

describe('Wait Utilities', () => {
    it('should resolve when condition is met', async () => {
        let val = 0;
        setTimeout(() => { val = 5; }, 50);

        const result = await waitForConditionEquals(
            () => val,
            5,
            { timeout: 5000, interval: 10 }
        );

        expect(result).toBe(5);
    });

    it('should throw with last value on timeout', async () => {
        const val = 'wrong-value';
        // String values are not quoted in the output unless they are objects
        await expect(
            waitForConditionEquals(
                () => val,
                'expected-value',
                { timeout: 100, interval: 10 }
            )
        ).rejects.toThrow(/Last value: wrong-value/);
    });

    it('should throw with truncated value for large objects', async () => {
        const largeStr = 'a'.repeat(300);

        const error = await waitForConditionEquals(
            () => largeStr,
            'something else',
            { timeout: 50, message: 'Should be something else' }
        ).then(() => null, (e: Error) => e);

        // The value is cut to 200 characters and flagged with an ellipsis.
        expect(error?.message).toContain('Should be something else (timed out after 50ms)');
        expect(error?.message.endsWith(`Last value: ${'a'.repeat(200)}...`)).toBe(true);
    });

    it('should support numeric assertions', async () => {
        let val = 10;
        setTimeout(() => { val = 4; }, 50);

        const result = await waitForConditionLessThan(
            () => val,
            5,
            { timeout: 5000, interval: 10 }
        );
        expect(result).toBe(4);
    });

    it('should support greater-than assertions', async () => {
        let val = 0;
        setTimeout(() => { val = 11; }, 50);

        const result = await waitForConditionGreaterThan(() => val, 10, { timeout: 5000, interval: 10 });
        expect(result).toBe(11);
    });

    it('should support not-equals assertions with an async getter', async () => {
        let val = 'initial';
        setTimeout(() => { val = 'changed'; }, 50);

        const result = await waitForConditionNotEquals(async () => val, 'initial', { timeout: 5000, interval: 10 });
        expect(result).toBe('changed');
    });

    it('should resolve truthy waits and reject falsy values until they change', async () => {
        let val: string | null = null;
        setTimeout(() => { val = 'ready'; }, 50);

        await expect(
            waitForConditionTruthy(() => val, { timeout: 5000, interval: 10 })
        ).resolves.toBe('ready');

        await expect(
            waitForConditionTruthy(() => 0, { timeout: 30, interval: 10 })
        ).rejects.toThrow(/Expected value to be truthy \(timed out after 30ms\)\nLast value: 0/);
    });

    it('should accept positional timeout, interval and message arguments', async () => {
        await expect(
            waitForConditionLessThan(() => 9, 5, 30, 10, 'Too big')
        ).rejects.toThrow(/Too big \(timed out after 30ms\)\nLast value: 9/);
    });

    it('should report the last getter error when it never produced a value', async () => {
        await expect(
            waitForConditionEquals(
                () => { throw new Error('getter exploded'); },
                1,
                { timeout: 30, interval: 10 }
            )
        ).rejects.toThrow(/Last error: getter exploded/);
    });

    it('should append onFailure debug info, including when onFailure itself throws', async () => {
        await expect(
            waitForConditionEquals(() => 1, 2, { timeout: 20, interval: 5, onFailure: () => 'extra context' })
        ).rejects.toThrow(/Debug Info: extra context/);

        await expect(
            waitForConditionEquals(() => 1, 2, {
                timeout: 20,
                interval: 5,
                onFailure: async () => { throw new Error('no debug'); }
            })
        ).rejects.toThrow(/Debug Info: \[Failed to generate: Error: no debug\]/);
    });

    it('should support the legacy predicate form', async () => {
        let ready = false;
        setTimeout(() => { ready = true; }, 30);

        await expect(waitForCondition(() => ready, 5000, 10)).resolves.toBeUndefined();
        await expect(waitForCondition(() => false, 20, 5, 'never ready')).rejects.toThrow(/never ready \(timed out after 20ms\)/);
    });
});
