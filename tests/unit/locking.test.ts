import { describe, it, expect, vi, beforeEach } from 'vitest';
import { measureClockSkew } from '../../src/locking';
import {
    collection,
    doc,
    setDoc,
    getDoc,
    deleteDoc,
    serverTimestamp,
} from '@firebase/firestore';

vi.mock('@firebase/firestore', () => ({
    collection: vi.fn(),
    doc: vi.fn(),
    setDoc: vi.fn(),
    getDoc: vi.fn(),
    deleteDoc: vi.fn(),
    serverTimestamp: vi.fn(),
}));

describe('locking', () => {
    describe('measureClockSkew', () => {
        const mockDb: any = {};
        const path = 'test-path';
        const uid = 'test-uid';

        beforeEach(() => {
            vi.clearAllMocks();
            (collection as any).mockReturnValue({});
            (doc as any).mockReturnValue({ id: 'mock-id' });
            (serverTimestamp as any).mockReturnValue({ _type: 'serverTimestamp' });
            (deleteDoc as any).mockResolvedValue(undefined);
        });

        it('should return the correct skew on happy path', async () => {
            const now = 1000000;
            const spy = vi.spyOn(Date, 'now').mockReturnValue(now);
            const serverTimeMillis = now + 5000;

            (setDoc as any).mockResolvedValue(undefined);
            (getDoc as any).mockResolvedValue({
                data: () => ({
                    t: {
                        toMillis: () => serverTimeMillis
                    }
                })
            });

            const skew = await measureClockSkew(mockDb, path, uid);

            expect(skew).toBe(5000);
            expect(setDoc).toHaveBeenCalled();
            expect(getDoc).toHaveBeenCalled();
            expect(deleteDoc).toHaveBeenCalled();

            spy.mockRestore();
        });

        it('should return 0 skew if setDoc fails', async () => {
            (setDoc as any).mockRejectedValue(new Error('Firestore error'));

            const skew = await measureClockSkew(mockDb, path, uid);

            expect(skew).toBe(0);
            expect(deleteDoc).toHaveBeenCalled();
        });

        it('should return 0 skew if getDoc fails', async () => {
            (setDoc as any).mockResolvedValue(undefined);
            (getDoc as any).mockRejectedValue(new Error('Firestore error'));

            const skew = await measureClockSkew(mockDb, path, uid);

            expect(skew).toBe(0);
            expect(deleteDoc).toHaveBeenCalled();
        });

        it('should return 0 skew if data is missing', async () => {
            (setDoc as any).mockResolvedValue(undefined);
            (getDoc as any).mockResolvedValue({
                data: () => undefined
            });

            const skew = await measureClockSkew(mockDb, path, uid);

            expect(skew).toBe(0);
            expect(deleteDoc).toHaveBeenCalled();
        });

        it('should return 0 skew if timestamp is missing in data', async () => {
            (setDoc as any).mockResolvedValue(undefined);
            (getDoc as any).mockResolvedValue({
                data: () => ({ somethingElse: true })
            });

            const skew = await measureClockSkew(mockDb, path, uid);

            expect(skew).toBe(0);
            expect(deleteDoc).toHaveBeenCalled();
        });

        it('should return 0 skew if timestamp is invalid', async () => {
            (setDoc as any).mockResolvedValue(undefined);
            (getDoc as any).mockResolvedValue({
                data: () => ({
                    t: { notAFunction: true }
                })
            });

            const skew = await measureClockSkew(mockDb, path, uid);

            expect(skew).toBe(0);
            expect(deleteDoc).toHaveBeenCalled();
        });
    });
});
