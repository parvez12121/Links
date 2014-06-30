/*
 * BitemporalDAOMybatisAutoGenImpl.java
 *
 */

/*
 * Copyright (c) 2013 D. E. Shaw &amp; Co., L.P. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of D. E. Shaw &amp; Co., L.P. ("Confidential Information")
 */

package deshaw.munshi.core.bt.dao;

import com.google.common.collect.Lists;

import deshaw.munshi.commons.util.DAOException;
import deshaw.munshi.core.bt.BitemporalEntriesGenerator;
import deshaw.munshi.core.bt.dao.mapper.BitemporalMapper;
import deshaw.munshi.core.bt.dao.util.BitemporalJdbcBatchQueryInput;
import deshaw.munshi.core.bt.dao.util.BitemporalQueryInput;
import deshaw.munshi.core.bt.domain.Bitemporal;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;

import org.mybatis.spring.support.SqlSessionDaoSupport;

/**
 * Mybatis implementation of {@link BaseBitemporalDAO} using Annotations
 * 
 * @author parvez
 */
public class BitemporalDAOMybatisAutoGenImpl<T extends Bitemporal<?>> extends SqlSessionDaoSupport implements BitemporalDAO<T> {
	private static final String KEY_STAGE_TABLE = "keyStageTable";

	private static final String STAGE_TABLE = "stageTable";
	private static final String STAGE_TABLE_DB = "stageTableDB";

	private static final String VALUES_STAGE_TABLE = "valuesStageTable";	

	private static final String BT_SELECT = "BT_SELECT";

	/**
     * The logger for this class
     */
    public final Logger LOG = Logger.getLogger(this.getClass());
    
    /**
     * The BT select batch size
     */
    private int selectQueryBatchSize = -1;

    /**
     * The BT merge batch size
     */
    private int mergeQueryBatchSize = -1;
    
    /**
     * The universe end date
     */
    protected Date universeEndDate = null;

    /**
     * If JDBC batch should be used
     */
    private boolean useJdbcBatch = false;
    
    /**
     * stores namespace of Dao
     */
    private String namespace;
    

    /**
     * DB where stage table should be created
     */
    private String stageTableDB = "sandbox";
	
    /**
     * returns mapper for current Sql Session
     * @return
     */
	public BitemporalMapper getBitemporalMapper() {
		if (! getSqlSession().getConfiguration().hasMapper(BitemporalMapper.class)) {
    		getSqlSession().getConfiguration().addMapper(BitemporalMapper.class);
    	}
		
		return getSqlSession().getMapper(BitemporalMapper.class);
	}
	
	/**
	 * retrieves id columns of result map
	 * @param resultMap
	 * @return
	 */
	public String [] getIdColumns(ResultMap resultMap) {
		List<ResultMapping> idMappings = resultMap.getIdResultMappings();
		String [] tmpidColumns = new String[idMappings.size()];
		int ptr = 0;
		for (ResultMapping item : idMappings) {
    		tmpidColumns[ptr++] = item.getColumn();
    	}
		
		return tmpidColumns;
	}
	
	/**
	 * retrieves id properties of result map
	 * @param resultMap
	 * @return
	 */
	public String [] getIdProperties(ResultMap resultMap) {
		List<ResultMapping> idMappings = resultMap.getIdResultMappings();
		String [] tmpIdProperties = new String[idMappings.size()];
		int ptr = 0;
		for (ResultMapping item : idMappings) {
    		tmpIdProperties[ptr++] = item.getProperty();
    	}
		
		return tmpIdProperties;
	}
	
	/**
	 * retrieves all columns of result map
	 * @param resultMap
	 * @return
	 */
	public String [] getColumns(ResultMap resultMap) {
		List<ResultMapping> allMappings = resultMap.getResultMappings();
		String [] tmpColumns = new String[allMappings.size()];
		int ptr = 0;
		for (ResultMapping item : allMappings) {
    		tmpColumns[ptr++] = item.getColumn();
    	}
		
		return tmpColumns;
	}
	
	/**
	 * retrieves all properties of result map
	 * @param resultMap
	 * @return
	 */
	public String [] getProperties(ResultMap resultMap) {
		List<ResultMapping> allMappings = resultMap.getResultMappings();
		String [] tmpProperties = new String[allMappings.size()];
		int ptr = 0;
		for (ResultMapping item : allMappings) {
    		tmpProperties[ptr++] = item.getProperty();
    	}
		
		return tmpProperties;
	}
    /**
     * Merges the input actions in the bitemporal table.
     * 
     * This method is capable of batching the queries based on selectBatchSize and mergeBatchSize properties. Setting
     * any value less than 1 for the above properties will result in getting all results in one batch.
     * 
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public void merge(List<T> actions, Date recordDate, boolean delete) throws DAOException {
        // Read the input state
        String [] idColumns;
    	String [] columns;
    	String [] idProperties;
    	String [] properties;
    	BitemporalMapper mapper;    	
    	
    	//Getting Object of mapper interface
    	mapper = getBitemporalMapper();
    			
    	ResultMap resultMap = getSqlSession().getConfiguration().getResultMap(getResultMap());
    	
    	//retrieving the list of id columns, all columns, id properties, all properties		
    	idColumns = getIdColumns(resultMap);
    	columns = getColumns(resultMap);
    	properties = getProperties(resultMap);
    	idProperties = getIdProperties(resultMap);
    			
        List<T> inputState = getInputState(actions, idColumns, columns, idProperties, properties, mapper);

        // Generate BT entries to post
        List<T> outputEntries = (List<T>) new BitemporalEntriesGenerator(universeEndDate).execute(
                (List<Bitemporal<?>>) inputState, (List<Bitemporal<?>>) actions, recordDate, delete);

        // Log
        if (LOG.isDebugEnabled()) {
            LOG.debug("\nInput State   : " + inputState + "\nActions       : " + actions + "\nDelete?       : "
                    + delete + "\nOutput Entries: " + outputEntries);
        }

        // Post the entries to database
        mergeBitemporalEntries(outputEntries, idColumns, columns, idProperties, properties, mapper);
    }

    /**
     * Returns the input records which will be affected by the actions. This method is capable of batching the queries
     * based on selectQueryBatchSize property. Setting any value less than 1 for selectQueryBatchSize will result in
     * getting all results in one batch.
     * 
     * @param actions
     *            The actions to perform bitemporally
     * @return The input state which is affected by the actions
     */
    protected List<T> getInputState(List<T> actions,  String [] idColumns, String [] columns, String [] idProperties, String [] properties, BitemporalMapper mapper) {
        if (useJdbcBatch) {
            return getInputStateJdbcBatched(actions, idColumns, columns, idProperties, properties, mapper);
        } else {
            return getInputStateIterateBatched(actions, idColumns, columns, idProperties, properties, mapper);
        }
    }
    protected List<T> getInputStateJdbcBatched(List<T> actions, String [] idColumns, String [] columns, String [] idProperties, String [] properties, BitemporalMapper mapper) {

        // Check if there is something to fetch
        if (actions.isEmpty()) {
            return new ArrayList<T>();
        }
        LOG.debug("Starting input state fetch jdbc batched");
        // Step 1: Create stage table
        String keyStageTableName = getStageTableName(getNamespace());
        Map<String, Object> stageTableCreationParams = new HashMap<>();
        stageTableCreationParams.put(KEY_STAGE_TABLE, keyStageTableName);
        stageTableCreationParams.put(STAGE_TABLE_DB, stageTableDB);
 
        createKeyStage(stageTableCreationParams, idColumns, mapper);
        LOG.debug("Stage keys table created " + keyStageTableName);

        // Step 2: Populate stage table
        Map<String, Object> stageTablePopulationParams = new HashMap<>();
        stageTablePopulationParams.put(KEY_STAGE_TABLE, keyStageTableName);
        stageTablePopulationParams.put(STAGE_TABLE_DB, stageTableDB);

        // XXX: Check if we should define a separate parameter to take the partition size for jdbc batch
        int partitionSize = selectQueryBatchSize;
        if (partitionSize < 0) {
            partitionSize = actions.size();
        }
        List<List<T>> partitions = Lists.partition(actions, partitionSize);
        for (List<T> partition : partitions) {
            for (T rec : partition) {
                stageTablePopulationParams.put("rec", rec);

                insertKeyStage(stageTablePopulationParams, rec, rec.getValidStart(), rec.getValidEnd(), idColumns, idProperties, mapper);
            }
            getSqlSession().flushStatements();
        }
        LOG.debug("Stage keys table populated " + keyStageTableName);

        // Step 3: Query input state using stage table
        BitemporalJdbcBatchQueryInput param = getBitemporalParams(keyStageTableName, idColumns, columns, mapper);
        List<T> inputStates = readInputState(param);
        LOG.debug("Inputs queried " + inputStates.size());

        // Step 4: Drop stage table created before
        // Gentlemen clean up after themselves
        Map<String, Object> stageTableDropParams = new HashMap<>();
        stageTableDropParams.put(STAGE_TABLE, keyStageTableName);
        stageTableDropParams.put(STAGE_TABLE_DB, stageTableDB);

        dropStage(stageTableDropParams, mapper);
        
        LOG.debug("Stage keys table dropped " + keyStageTableName);

        return inputStates;
    }

    /**
     * Fetches the input state using the iterate in a batch technique (limited by 2000 params)
     * 
     * @param actions
     * @return
     */
    private List<T> getInputStateIterateBatched(List<T> actions, String [] idColumns, String [] columns, String [] idProperties, String [] properties, BitemporalMapper mapper) {
        List<T> output = new ArrayList<T>();
        int batchSize = (selectQueryBatchSize < 1) ? actions.size() : selectQueryBatchSize;
        LOG.info("Reading input state. Total actions:" + actions.size());
        for (int i = 0; i < actions.size(); i += batchSize) {
            int batchStart = i;
            int batchEnd = i + batchSize;

            int fromIndex = batchStart;
            int toIndex = Math.min(batchEnd, actions.size());

            LOG.debug("Reading input state. FromIndex:" + fromIndex + " ToIndex:" + toIndex);
            List<T> batch = actions.subList(fromIndex, toIndex);

            BitemporalQueryInput param = new BitemporalQueryInput(batch, universeEndDate);
            output.addAll(readInputState(param));
        }
        return output;
    }
    
    /**
     * Updates or inserts the entries to database bitemporally
     * 
     * @param entries
     *            The entries to update
     */
    protected void mergeBitemporalEntries(List<T> entries, String [] idColumns, String [] columns, String [] idProperties, String [] properties, BitemporalMapper mapper) {
        if (useJdbcBatch) {
            mergeBitemporalEntriesJdbcBatched(entries, idColumns, columns, idProperties, properties, mapper);
        } else {
        mergeBitemporalEntriesIterateBatched(entries, idColumns, columns, idProperties, properties, mapper);
        }
    }

    /**
     * Merges the entries using jdbc batched. This involves staging values to a staging table and then firing a query to
     * perform the merge, and being a good guy - cleaning up after ourselves
     * 
     * @param entries
     */
    protected void mergeBitemporalEntriesJdbcBatched(List<T> entries, String [] idColumns, String [] columns, String [] idProperties, String [] properties, BitemporalMapper mapper) {
        // Check if there is something to merge
        if (entries.isEmpty()) {
            return;
        }
        LOG.debug("Starting input state fetch jdbc batched");
        // Step 1: Create stage table
        String valuesStageTableName = getStageTableName(getNamespace());
        Map<String, Object> stageTableCreationParams = new HashMap<>();
        stageTableCreationParams.put(VALUES_STAGE_TABLE, valuesStageTableName);
        stageTableCreationParams.put(STAGE_TABLE_DB, stageTableDB);
        createValuesStage(stageTableCreationParams, columns, mapper);
        LOG.debug("Stage values table created " + valuesStageTableName);

        // Step 2: Populate stage table
        Map<String, Object> stageTablePopulationParams = new HashMap<>();
        stageTablePopulationParams.put(VALUES_STAGE_TABLE, valuesStageTableName);
        stageTablePopulationParams.put(STAGE_TABLE_DB, stageTableDB);

        // XXX: Check if we should define a separate parameter to take the partition size for jdbc batch
        int partitionSize = mergeQueryBatchSize;
        if (partitionSize < 0) {
            partitionSize = entries.size();
        }
        List<List<T>> partitions = Lists.partition(entries, partitionSize);
        for (List<T> partition : partitions) {
            for (T rec : partition) {
                stageTablePopulationParams.put("rec", rec);
                insertValuesStage(stageTablePopulationParams, rec, idColumns, columns, idProperties, properties, mapper);
            }
            getSqlSession().flushStatements();
        }
        LOG.debug("Stage values table populated " + valuesStageTableName);

        // Step 3: Merge entries using stage table
        BitemporalJdbcBatchQueryInput param = new BitemporalJdbcBatchQueryInput(universeEndDate, null,
                valuesStageTableName, stageTableDB);
        mergeOutputState(param, idColumns, columns, mapper);
        LOG.debug("Entries merged " + entries.size());

        // Step 4: Drop stage table created before
        // Gentlemen clean up after themselves
        Map<String, Object> stageTableDropParams = new HashMap<>();
        stageTableDropParams.put(STAGE_TABLE, valuesStageTableName);
        stageTableDropParams.put(STAGE_TABLE_DB, stageTableDB);
        dropStage(stageTableDropParams, mapper);
        LOG.debug("Stage values table dropped " + valuesStageTableName);

    }

        /**
     * This method is capable of batching the queries based on mergeQueryBatchSize property. Setting any value less than
     * 1 for mergeQueryBatchSize will result in getting all results in one batch.
     * 
     * @param entries
     *            the entries to update
     */
    private void mergeBitemporalEntriesIterateBatched(List<T> entries, String [] idColumns, String [] columns, String [] idProperties, String [] properties, BitemporalMapper mapper) {
        int batchSize = (mergeQueryBatchSize < 1) ? entries.size() : mergeQueryBatchSize;
        LOG.info("Merging. Total Entries:" + entries.size());
        for (int i = 0; i < entries.size(); i += batchSize) {
            int batchStart = i;
            int batchEnd = i + batchSize;

            int fromIndex = batchStart;
            int toIndex = Math.min(batchEnd, entries.size());

            LOG.debug("Merging. FromIndex:" + fromIndex + " ToIndex:" + toIndex);
            List<T> batch = entries.subList(fromIndex, toIndex);

            BitemporalQueryInput param = new BitemporalQueryInput(batch, universeEndDate);
            mergeOutputState(param, idColumns, columns, mapper);
        }
    }

    /**
     * @return the universeEndDate
     */
    public Date getUniverseEndDate() {
        return universeEndDate;
    }

    /**
     * @param universeEndDate
     *            the universeEndDate to set
     */
    public void setUniverseEndDate(Date universeEndDate) {
        this.universeEndDate = universeEndDate;
    }

    /**
     * @return the selectQueryBatchSize
     */
    public int getSelectQueryBatchSize() {
        return selectQueryBatchSize;
    }

    /**
     * @param selectQueryBatchSize
     *            the selectQueryBatchSize to set
     */
    public void setSelectQueryBatchSize(int selectQueryBatchSize) {
        this.selectQueryBatchSize = selectQueryBatchSize;
    }

    /**
     * @return the mergeQueryBatchSize
     */
    public int getMergeQueryBatchSize() {
        return mergeQueryBatchSize;
    }

    /**
     * @param mergeQueryBatchSize
     *            the mergeQueryBatchSize to set
     */
    public void setMergeQueryBatchSize(int mergeQueryBatchSize) {
        this.mergeQueryBatchSize = mergeQueryBatchSize;
    }

    /**
     * Returns the configured namespace
     * 
     * @return
     */
    public String getNamespace() {
        return namespace;
    }
    public void setNamespace(String namespace) {
    	this.namespace = namespace;
    }

    //not able to remove
    public void setUseJdbcBatch(boolean useJdbcBatch) {
        this.useJdbcBatch = useJdbcBatch;
    }
    
    public String getResultMap() {
    	return "resultmap";
    }
    
    public String getStageTableDB() {
    	return stageTableDB;
    }
    
    public String getTableName() {
    	return "value returned from user dao";
    }
    
    /**
     * creates Key Stage Table
     * @param stageTableCreationParams includes various variables which are required by BT_STAGE_KEY_CREATE
     */
    public void createKeyStage(Map<String, Object> stageTableCreationParams, String [] idColumns,BitemporalMapper mapper) {
    	mapper.btStageKeyCreate(stageTableDB, (String) stageTableCreationParams.get(KEY_STAGE_TABLE), idColumns, getStageTableDB(), getTableName());
    }
    
    /**
     * insert values into Key Stage Table
     * @param stageTablePopulationnParams includes various variables which are required by BT_STAGE_KEY_INSERT
     * @param rec stores values of Object to be inserted into database
     * @param validStart beginning point of valid time interval
     * @param validEnd ending point of valid time interval
     */
    public void insertKeyStage(Map<String, Object> stageTablePopulationParams, Object rec, Date validStart, Date validEnd, String [] idColumns, String [] idProperties, BitemporalMapper mapper) {
    	mapper.btStageKeyInsert(stageTableDB, (String) stageTablePopulationParams.get(KEY_STAGE_TABLE), idColumns, idProperties, rec, validStart, validEnd);
    }
    
    /**
     * drops Stage Tables
     * @param stageTableDropParams includes various variables which are required by BT_STAGE_DROP
     */
    public void dropStage(Map<String, Object> stageTableDropParams, BitemporalMapper mapper) {
    	mapper.btStageDrop(stageTableDB, (String) stageTableDropParams.get(STAGE_TABLE));
    }
    
    /**
     * creates Values Stage Table
     * @param stageTableCreationParams includes various variables which are required by BT_STAGE_VALUES_CREATE
     */
    public void createValuesStage(Map<String, Object> stageTableCreationParams, String [] columns, BitemporalMapper mapper) {
    	mapper.btStageValuesCreate(stageTableDB, (String) stageTableCreationParams.get(VALUES_STAGE_TABLE), columns, getStageTableDB(), getTableName());
    }
    
    /**
     * inserts values into Values Stage Table
     * @param stageTablePopulationParams includes various variables which are required by BT_STAGE_VALUES_INSERT
     * @param rec stores values of Object to be inserted into database
     */
    public void insertValuesStage(Map<String, Object> stageTablePopulationParams, Object rec, String [] idColumns, String [] columns, String [] idProperties, String [] properties, BitemporalMapper mapper) {
    	mapper.btStageValuesInsert(stageTableDB, (String) stageTablePopulationParams.get(VALUES_STAGE_TABLE), columns, properties, rec);
    }
    
    /**
     * updates and inserts values in the User Table
     * @param param contains various values to be inserted into database
     */
    public void mergeOutputState(BitemporalQueryInput param, String [] idColumns, String [] columns, BitemporalMapper mapper) {
    	BitemporalJdbcBatchQueryInput valueStage = (BitemporalJdbcBatchQueryInput) param;
        mapper.btUpdateInsert(universeEndDate, stageTableDB, valueStage.getValuesStageTable(), columns, getDBAndTable());
    }
    
    /**
     * Return BitemporalJdbcBatchQueryInput for select query
     * @param Key Stage Table Name
     */
    public BitemporalJdbcBatchQueryInput getBitemporalParams(String keyStageTableName, String [] idColumns, String [] columns, BitemporalMapper mapper) {
    	return new BitemporalJdbcBatchQueryInput(
				universeEndDate, keyStageTableName, null, stageTableDB, idColumns, columns, getDBAndTable());
    }
    
    /**
     * Return List of Domain Objects user specified
     * @param param contains parameters for select records from database
     */
    public List<T> readInputState(BitemporalQueryInput param) {
        return getSqlSession().selectList(getNamespace() + "." + BT_SELECT, param);
    }
    
    public String getDBAndTable() {
    	return "DB and Table Name";
    }
    
    private static String getStageTableName(String templateTable) {
        String stageTableName = templateTable + "_" + new Date().getTime() + "_" + Thread.currentThread().getId();
        return stageTableName;
    }

    
	@Override
	public List<T> getOverlappingEntries(List<T> actions) throws DAOException {
		return getInputState(actions, null, null, null, null, null);
	}

	 /**
     * {@inheritDoc}
     */
    @Override
    public void merge(List<T> actions) throws DAOException {
        merge(actions, new Date());

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void merge(List<T> actions, Date recordDate) throws DAOException {
        merge(actions, recordDate, false);
    }

    /**
     * Deletes the input actions in the bitemporal table. The value from new Date() will be used as record date.
     * 
     * @param actions
     */
    @Override
    public void delete(List<T> actions) throws DAOException {
        delete(actions, new Date());
    }

    /**
     * Deletes the input actions in the bitemporal table.
     * 
     * @param actions
     * @param recordDate
     */
    @Override
    public void delete(List<T> actions, Date recordDate) throws DAOException {
        merge(actions, recordDate, true);
    }

}
