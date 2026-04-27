import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { checkLockStatus, LockConfig } from '../../src/locking';
import * as firestore from '@firebase/firestore';

vi.mock('@firebase/firestore', () => ({
    doc: vi.fn(),
    getDoc: vi.fn(),
    collection: vi.fn(),
    setDoc: vi.fn(),
    deleteDoc: vi.fn(() => Promise.resolve()),
    serverTimestamp: vi.fn(() => ({ toMillis: () => Date.now() })),
}));

describe('locking - checkLockStatus', () => {
    const db = {} as any;
    const path = 'test-path';
    const uid = 'test-uid';
    const lockTTL = 60000; // 60 seconds

    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('should return exists: false when lock document does not exist', async () => {
        vi.mocked(firestore.getDoc).mockResolvedValue({
            exists: () => false
        } as any);

        const config: LockConfig = { db, path, uid, lockTTL, cachedClockOffset: 0 };
        const status = await checkLockStatus(config);

        expect(status).toEqual({ exists: false });
    });

    it('should return lock info when lock exists and is not expired', async () => {
        const now = Date.now();
        const createdAt = now - 30000; // 30 seconds ago

        vi.mocked(firestore.getDoc).mockResolvedValue({
            exists: () => true,
            data: () => ({
                owner: 'other-owner',
                createdAt: { toMillis: () => createdAt }
            })
        } as any);

        const config: LockConfig = { db, path, uid, lockTTL, cachedClockOffset: 0 };
        const status = await checkLockStatus(config);

        expect(status.exists).toBe(true);
        expect(status.owner).toBe('other-owner');
        expect(status.isExpired).toBe(false);
        expect(status.ageMs).toBe(30000);
    });

    it('should return isExpired: true when lock age exceeds TTL', async () => {
        const now = Date.now();
        const createdAt = now - 90000; // 90 seconds ago

        vi.mocked(firestore.getDoc).mockResolvedValue({
            exists: () => true,
            data: () => ({
                owner: 'other-owner',
                createdAt: { toMillis: () => createdAt }
            })
        } as any);

        const config: LockConfig = { db, path, uid, lockTTL, cachedClockOffset: 0 };
        const status = await checkLockStatus(config);

        expect(status.isExpired).toBe(true);
        expect(status.ageMs).toBe(90000);
    });

    it('should respect cachedClockOffset in age calculation', async () => {
        const now = Date.now();
        const createdAt = now - 30000;
        const cachedClockOffset = 10000; // Server is 10s ahead

        vi.mocked(firestore.getDoc).mockResolvedValue({
            exists: () => true,
            data: () => ({
                owner: 'other-owner',
                createdAt: { toMillis: () => createdAt }
            })
        } as any);

        const config: LockConfig = { db, path, uid, lockTTL, cachedClockOffset };
        const status = await checkLockStatus(config);

        // serverNow = now + 10000
        // ageMs = (now + 10000) - (now - 30000) = 40000
        expect(status.ageMs).toBe(40000);
    });

    it('should handle missing createdAt by defaulting age to a large value (expired)', async () => {
        vi.mocked(firestore.getDoc).mockResolvedValue({
            exists: () => true,
            data: () => ({
                owner: 'other-owner'
                // createdAt missing
            })
        } as any);

        const config: LockConfig = { db, path, uid, lockTTL, cachedClockOffset: 0 };
        const status = await checkLockStatus(config);

        expect(status.exists).toBe(true);
        expect(status.isExpired).toBe(true);
        expect(status.ageMs).toBe(Date.now()); // Date.now() - 0
    });

    it('should return exists: false when Firestore throws an error', async () => {
        vi.mocked(firestore.getDoc).mockRejectedValue(new Error('Firestore error'));

        const config: LockConfig = { db, path, uid, lockTTL, cachedClockOffset: 0 };
        const status = await checkLockStatus(config);

        expect(status).toEqual({ exists: false });
    });

    it('should call measureClockSkew when cachedClockOffset is undefined', async () => {
        const now = Date.now();
        const createdAt = now - 30000;

        // Mock getDoc for the lock itself
        vi.mocked(firestore.getDoc).mockImplementation(async (ref: any) => {
            if (ref && ref.path && ref.path.includes('skew')) {
                // Mock for measureClockSkew's internal getDoc
                return {
                    exists: () => true,
                    data: () => ({ t: { toMillis: () => now + 5000 } }) // Server is 5s ahead
                } as any;
            }
            return {
                exists: () => true,
                data: () => ({
                    owner: 'other-owner',
                    createdAt: { toMillis: () => createdAt }
                })
            } as any;
        });

        // Mock doc and collection for measureClockSkew
        vi.mocked(firestore.collection).mockReturnValue({ path: 'maint' } as any);
        vi.mocked(firestore.doc).mockImplementation(((...args: any[]) => {
            if (args.length === 1) return { id: 'auto-id', path: 'maint/auto-id' };
            return { path: args.join('/') };
        }) as any);
        vi.mocked(firestore.setDoc).mockResolvedValue(undefined);

        const config: LockConfig = { db, path, uid, lockTTL, cachedClockOffset: undefined };
        const status = await checkLockStatus(config);

        // serverOffset should be 5000
        // ageMs = (now + 5000) - (now - 30000) = 35000
        expect(status.ageMs).toBe(35000);
        expect(firestore.setDoc).toHaveBeenCalled();
    });
});
