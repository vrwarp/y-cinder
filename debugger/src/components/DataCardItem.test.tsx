// @vitest-environment jsdom
import React from 'react';
import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, cleanup } from '@testing-library/react';
import { DataCardItem } from './DataCardItem';
import * as matchers from '@testing-library/jest-dom/matchers';

expect.extend(matchers);

const mockTheme = {
    text: '#000',
    textMuted: '#666',
    bg: '#fff',
    primary: '#00f',
    border: '#ccc',
};

const mockPreStyle = {
    padding: '10px'
};

const mockRenderData = vi.fn((data: any, limit: number) => JSON.stringify(data));

describe('DataCardItem', () => {
    afterEach(() => {
        cleanup();
    });

    it('renders correctly with given properties', () => {
        const data = {
            createdAt: { seconds: 1234567890 },
            createdBy: 'test-user',
            clientIDs: ['clientA'],
            clientClocks: ['100'],
        };

        render(
            <DataCardItem
                data={data}
                renderData={mockRenderData}
                theme={mockTheme}
                preStyle={mockPreStyle}
            />
        );

        // Verify creator text
        expect(screen.getByText(/test-user/)).toBeInTheDocument();
    });

    it('can toggle raw JSON view', () => {
        const data = { id: 'test-id', someData: 'foo' };
        render(
            <DataCardItem
                data={data}
                renderData={mockRenderData}
                theme={mockTheme}
                preStyle={mockPreStyle}
            />
        );

        const toggleBtn = screen.getByRole('button', { name: /Show Raw JSON/i });
        expect(toggleBtn).toBeInTheDocument();

        fireEvent.click(toggleBtn);
        expect(screen.getByRole('button', { name: /Hide Raw JSON/i })).toBeInTheDocument();

        // The pre output from mockRenderData should be there
        expect(screen.getByText(JSON.stringify(data))).toBeInTheDocument();
    });

    it('detects and shows offloaded snapshot badges', () => {
        const data = {
            snapshotStoragePath: 'gs://test/path',
            content: new Uint8Array([1, 2, 3])
        };
        render(
            <DataCardItem
                data={data}
                renderData={mockRenderData}
                theme={mockTheme}
                preStyle={mockPreStyle}
            />
        );

        expect(screen.getByText('OFFLOADED')).toBeInTheDocument();
    });

    it('renders pending structs warning when present', () => {
        const data = {
            __pendingStructs: {
                count: 5,
                note: 'missing dependencies',
                preview: [
                    { class: 'ItemString', client: 123, clock: 0 }
                ]
            }
        };
        render(
            <DataCardItem
                data={data}
                renderData={mockRenderData}
                theme={mockTheme}
                preStyle={mockPreStyle}
            />
        );

        expect(screen.getByText(/Pending Structs Detected \(5\)/)).toBeInTheDocument();
        expect(screen.getByText('missing dependencies')).toBeInTheDocument();

        // Reveal the pending structs details via the banner's toggle
        fireEvent.click(screen.getByRole('button', { name: /Show Details/i }));

        expect(screen.getByText(/ItemString\s*\|\s*client:\s*123\s*\|\s*clock:\s*0/)).toBeInTheDocument();
    });
});
