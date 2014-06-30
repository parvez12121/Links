/*
 * Copyright (c) 2012 D. E. Shaw & Co., L.P. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of D. E. Shaw & Co., L.P. ("Confidential Information")
 */

package deshaw.munshi.core.bt.dao.mapper;

import java.util.Date;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

import deshaw.munshi.core.bt.dao.BitemporalDAOMybatisAutoGenImpl;

/**
 * Generates Dynamic Sql Queries Using Annotations for {@link BitemporalDAOMybatisAutoGenImpl}
 * @author parvez
 *
 */
public interface BitemporalMapper {
	@Insert(  "SELECT "
			+ "<foreach item='item' index='index' collection='keyColumns' open=' ' separator=', ' close=', '> "
			+ "${item} "  
			+ "</foreach> "
			+ "vt_begin, vt_end "
			+ "INTO ${stageTableDB}..${keyStageTableName} "
			+ "FROM ${tableDB}..${keyTableName} " 
			+ "WHERE 1=2 "
			+ "")
	void btStageKeyCreate(@Param("stageTableDB") String stageTableDB, @Param("keyStageTableName") String keyStageTableName, @Param("keyColumns") String [] keyColumns, @Param("tableDB") String tableDB, @Param("keyTableName") String keyTableName); 
	
	@Insert(  "SELECT "
			+ "<foreach item='item' index='index' collection='allColumns' open=' ' separator=', ' close=' '> "
			+ "${item} "  
			+ "</foreach> "
			+ "INTO ${stageTableDB}..${valuesStageTableName} "
			+ "FROM ${tableDB}..${valuesTableName} " 
			+ "WHERE 1=2 "
			+ "")
	void btStageValuesCreate(@Param("stageTableDB") String stageTableDB, @Param("valuesStageTableName") String valuesStageTableName, @Param("allColumns") String [] allColumns, @Param("tableDB") String tableDB, @Param("valuesTableName") String valuesTableName); 
	
	@Insert(  "INSERT  "
			+ "INTO ${stageTableDB}..${keyStageTableName} "
			+ "<foreach item='item' index='index' collection='keyColumns' open='(' separator=', ' close=', ' > "
			+ "${item} "
			+ "</foreach> "
			+ "vt_begin, vt_end ) "
			+ "VALUES "
			+ "<foreach item='item' index='index' collection='keyColumnValues' open='(' separator=', ' close=', ' > "
			+ " #{rec.${item}} "
			+ "</foreach> "
			+ "#{validStart}, #{validEnd} ) "
		    + "")
	void btStageKeyInsert(@Param("stageTableDB") String stageTableDB, @Param("keyStageTableName") String keyStageTableName, @Param("keyColumns") String [] keyColumns, @Param("keyColumnValues") String [] keyColumnValues, @Param("rec") Object rec, @Param("validStart") Date validStart, @Param("validEnd") Date validEnd);
	
	
	@Insert(  "INSERT  INTO ${stageTableDB}..${valuesStageTable} "
			+ "<foreach item='item' index='index' collection='allColumns' open=' ( ' separator=' , ' close=' ) ' > "
			+ "${item} "
			+ "</foreach> "
			+ "VALUES "
			+ "<foreach item='item' index='index' collection='allProperties' open=' ( ' separator=' , ' close=' ) ' > "
			+ "#{rec.${item}} "
			+ "</foreach> "
			+ "")
	void btStageValuesInsert(@Param("stageTableDB") String stageTableDB, @Param("valuesStageTable") String valuesStageTable, @Param("allColumns") String [] allColumns, @Param("allProperties") String [] allProperties, @Param("rec") Object rec);
	
	
	@Delete("drop table ${stageTableDB}..${stageTableName}")
	void btStageDrop(@Param("stageTableDB") String stageTableDB, @Param("stageTableName") String stageTableName);
	
	
	@Update(  "UPDATE a "
			+ "SET tt_end = b.tt_end "
			+ "FROM ${dBAndTable} a "
			+ "JOIN ${stageTableDB}..${valuesStageTable} b "
			+ "ON "
			+ "<foreach item='item' index='index' collection='allColumns' open=' ' separator=' AND ' close=' ' > "
			+ "<if test='item != \"tt_end\"'> a.${item} = b.${item}</if> "
			+ "</foreach> "
			+ "WHERE b.tt_end != #{universeEndDate} " 
			+ "   "
			+ "INSERT INTO ${dBAndTable}  "
			+ "<foreach item='item' index='index' collection='allColumns' open=' ( ' separator=' , ' close=' ) ' > "
			+ "${item} "
			+ "</foreach> "
			+ "SELECT "
			+ "<foreach item='item' index='index' collection='allColumns' open='  ' separator=' , ' close='  ' > "
			+ "  ${item} "
			+ "</foreach> "
			+ "FROM ${stageTableDB}..${valuesStageTable} b "
			+ "WHERE tt_end = #{universeEndDate}"
			+ "")
	void btUpdateInsert(@Param("universeEndDate") Date universeEndDate, @Param("stageTableDB") String stageTableDB, @Param("valuesStageTable") String valuesStageTable, @Param("allColumns") String [] allColumns, @Param("dBAndTable") String dBAndTable);
}
