import { describe, it, expect } from 'vitest';
import { parseFirebaseConfig } from './config-parser';

describe('parseFirebaseConfig', () => {
    it('should parse a valid JavaScript object notation string', () => {
        const input = `const firebaseConfig = {
      apiKey: "some-api-key",
      authDomain: "demo.firebaseapp.com",
      projectId: "demo-project",
      storageBucket: "demo-project.appspot.com",
      appId: "1:1234:web:567"
    };`;
        const result = parseFirebaseConfig(input);
        expect(result).toEqual({
            apiKey: 'some-api-key',
            authDomain: 'demo.firebaseapp.com',
            projectId: 'demo-project',
            storageBucket: 'demo-project.appspot.com',
            appId: '1:1234:web:567'
        });
    });

    it('should parse raw object literal without const declaration', () => {
        const input = `{
      apiKey: "test-key",
      projectId: "test-project"
    }`;
        const result = parseFirebaseConfig(input);
        expect(result).toEqual({
            apiKey: 'test-key',
            projectId: 'test-project'
        });
    });

    it('should handle single quotes', () => {
        const input = `{
      projectId: 'my-project-id'
    }`;
        const result = parseFirebaseConfig(input);
        expect(result.projectId).toBe('my-project-id');
    });

    it('should throw an error for completely invalid inputs', () => {
        expect(() => parseFirebaseConfig('invalid-data')).toThrowError(/Failed to parse configuration/);
    });

    it('should return null for empty input', () => {
        expect(parseFirebaseConfig('')).toBeNull();
    });
});
