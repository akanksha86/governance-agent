export interface MetadataEvent {
  id: string;
  timestamp: string;
  type: 'CREATE' | 'UPDATE' | 'ASPECT_ADD' | 'SCHEMA_CHANGE';
  user: string;
  summary: string;
  snapshot: any;
}

export const mockEvolutionData: MetadataEvent[] = [
  {
    id: '1',
    timestamp: '2026-03-09T10:00:00Z',
    type: 'CREATE',
    user: 'system-harvest',
    summary: 'Initial table discovery',
    snapshot: {
      fields: [
        { name: 'customer_id', type: 'STRING' },
        { name: 'name', type: 'STRING' },
        { name: 'email', type: 'STRING' }
      ],
      aspects: {}
    }
  },
  {
    id: '2',
    timestamp: '2026-03-09T14:15:00Z',
    type: 'SCHEMA_CHANGE',
    user: 'admin@akankshapb.altostrat.com',
    summary: 'Added migration_flag column',
    snapshot: {
      fields: [
        { name: 'customer_id', type: 'STRING' },
        { name: 'name', type: 'STRING' },
        { name: 'email', type: 'STRING' },
        { name: 'migration_flag', type: 'BOOLEAN' }
      ],
      aspects: {}
    }
  },
  {
    id: '3',
    timestamp: '2026-03-09T15:30:00Z',
    type: 'ASPECT_ADD',
    user: 'admin@akankshapb.altostrat.com',
    summary: 'Associated data-governance-aspect',
    snapshot: {
      fields: [
        { name: 'customer_id', type: 'STRING' },
        { name: 'name', type: 'STRING' },
        { name: 'email', type: 'STRING' },
        { name: 'migration_flag', type: 'BOOLEAN' }
      ],
      aspects: {
        'data-governance-aspect': {
          owner: 'Data Governance Team',
          classification: 'PII'
        }
      }
    }
  }
];
