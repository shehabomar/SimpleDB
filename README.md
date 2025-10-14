# SimpleDB

A lightweight relational database management system implemented in Java as part of a Database Systems course. SimpleDB provides core database functionality including storage management, query processing, transaction handling, and indexing.

## Project Overview

SimpleDB is designed as an educational database system that implements fundamental database concepts from scratch. It supports basic SQL operations, ACID transactions, and efficient query execution.

## Current Features

### Storage Management
- **HeapFile**: Page-based heap file storage for tables
- **B-Tree Index**: B-Tree implementation for efficient indexed access
  - B-Tree leaf pages, internal pages, and header pages
  - Root pointer page management
  - Next-key locking support
- **Buffer Pool**: Page caching and eviction management
- **Catalog**: Table and schema management

### Data Types and Structures
- **Type System**: Support for integers and strings
- **Tuple Management**: Record structure with field handling
- **TupleDesc**: Schema definition and metadata
- **PageId/RecordId**: Unique identifiers for pages and records

### Query Processing
- **Query Operators**:
  - `SeqScan`: Sequential table scanning
  - `Filter`: Selection operations with predicates
  - `Join`: Nested loop join and hash equi-join
  - `Aggregate`: Grouping and aggregation operations
  - `OrderBy`: Sorting operations
  - `Project`: Projection operations
  - `Insert/Delete`: Data modification operations

- **Query Optimization**:
  - Cost-based query optimization
  - Join order optimization
  - Cardinality estimation
  - Statistics collection (histograms for selectivity estimation)
  - Query plan visualization

### Transaction Management
- **Transaction Support**: Full ACID properties
- **Concurrency Control**: Page-level locking
- **Deadlock Detection**: Timeout-based and dependency graph detection
- **Write-Ahead Logging (WAL)**: Recovery and durability support
- **Transaction Abort and Recovery**: Rollback capabilities

### Query Interface
- **SQL Parser**: ZQL-based SQL query parsing
- **Command-Line Interface**: Interactive query execution
- **File Utilities**: 
  - Text to binary conversion for data files
  - Table printing and inspection tools

### Testing Infrastructure
- Comprehensive unit tests for all major components
- System tests for end-to-end functionality
- Performance and stress tests
- Test utilities and helper classes

## Project Structure

```
SimpleDB/
├── src/java/simpledb/      # Core implementation source files
├── test/simpledb/          # Unit and system tests
├── bin/                    # Compiled classes
├── dist/                   # Distribution JAR files
├── lib/                    # External dependencies
└── build.xml               # Ant build configuration
```

## Building and Running

### Build the Project
```bash
ant compile
```

### Build JAR Distribution
```bash
ant dist
```

### Run Tests
```bash
# Run all unit tests
ant test

# Run all system tests
ant systemtest

# Run a specific test
ant runtest -Dtest=HeapFileReadTest

# Run a specific system test
ant runsystest -Dtest=ScanTest
```

### Command-Line Usage

**Convert text file to database format:**
```bash
java -jar dist/simpledb.jar convert data.txt 3 int,string,int
```

**Print table contents:**
```bash
java -jar dist/simpledb.jar print data.dat 3
```

**Run SQL parser (if implemented):**
```bash
java -jar dist/simpledb.jar parser [catalog_file]
```

## Dependencies

- **Java 8+**: Core language runtime
- **JUnit 4.5**: Testing framework
- **ZQL**: SQL parsing library
- **JLine 0.9.94**: Command-line editing
- **Apache MINA**: Network application framework
- **SLF4J/Log4j**: Logging infrastructure
- **Ant**: Build automation

## Development

### Generate Eclipse Project Files
```bash
ant eclipse
```

### Generate Javadoc Documentation
```bash
ant javadocs
```

### Clean Build Artifacts
```bash
ant clean
```

## Testing

The project includes extensive test coverage:
- **Unit Tests**: Test individual components in isolation
- **System Tests**: End-to-end integration tests
- **Performance Tests**: Verify system behavior under load

Test reports can be generated with:
```bash
ant test-report
```

## Future Features

### Performance Enhancements
- [ ] **Query Execution Optimization**
  - Implement pipelined query execution
  - Add support for sort-merge join
  - Implement hash join for better performance
  - Add parallel query execution support

- [ ] **Index Improvements**
  - Hash index implementation
  - Composite/multi-column indexes
  - Full-text search indexes
  - Index compression

- [ ] **Advanced Buffer Pool**
  - Implement LRU-K or CLOCK eviction policies
  - Add buffer pool monitoring and statistics
  - Implement adaptive replacement cache (ARC)
  - Pre-fetching and read-ahead support

### Extended Functionality
- [ ] **Data Types**
  - Support for floating-point numbers (FLOAT, DOUBLE)
  - Date and time types (DATE, TIME, TIMESTAMP)
  - BLOB/CLOB for large objects
  - NULL value handling
  - User-defined types

- [ ] **SQL Features**
  - Complete SQL-92 compliance
  - Subquery support (correlated and uncorrelated)
  - Views and materialized views
  - Window functions (RANK, ROW_NUMBER, etc.)
  - Common Table Expressions (CTEs)
  - UNION, INTERSECT, EXCEPT operations

- [ ] **Advanced Query Processing**
  - Query result caching
  - Prepared statement support
  - Query rewriting and normalization
  - Adaptive query execution
  - Runtime statistics feedback

### Storage and Indexing
- [ ] **Column-Oriented Storage**
  - Columnar storage format for analytics
  - Compression schemes (dictionary, RLE, bit-packing)
  
- [ ] **Partitioning**
  - Range partitioning
  - Hash partitioning
  - List partitioning

### Concurrency and Recovery
- [ ] **Advanced Concurrency Control**
  - Row-level locking instead of page-level
  - Multi-version concurrency control (MVCC)
  - Optimistic concurrency control
  - Snapshot isolation

- [ ] **Enhanced Recovery**
  - Checkpointing improvements
  - ARIES recovery algorithm
  - Online backup and restore
  - Point-in-time recovery

### System Features
- [ ] **Security**
  - User authentication and authorization
  - Role-based access control (RBAC)
  - SQL injection prevention
  - Encryption at rest and in transit

- [ ] **Networking**
  - Client-server architecture
  - JDBC driver implementation
  - Connection pooling
  - Distributed query processing

- [ ] **Monitoring and Administration**
  - Database statistics and monitoring dashboard
  - Query profiling and EXPLAIN ANALYZE
  - Slow query log
  - Performance tuning advisors
  - Automated schema suggestions

### Data Import/Export
- [ ] **File Format Support**
  - CSV import/export
  - JSON import/export
  - XML import/export
  - Parquet format support

### Advanced Features
- [ ] **Stored Procedures and Functions**
  - User-defined functions (UDF)
  - Stored procedures
  - Triggers

- [ ] **Replication**
  - Master-slave replication
  - Multi-master replication
  - Log shipping

- [ ] **Distributed Database**
  - Sharding support
  - Distributed transactions (2PC)
  - Cross-shard query execution

## Contributing

This is an educational project. When making contributions:
1. Follow existing code style and conventions
2. Add unit tests for new features
3. Update documentation as needed
4. Ensure all tests pass before submitting

## License

Educational project for Database Systems coursework.

## Acknowledgments

This project is based on the MIT 6.830/6.814 Database Systems course materials.

