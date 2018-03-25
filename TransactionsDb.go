package main

import (
	"database/sql"
	"encoding/json"
	"log"

	_ "github.com/denisenkom/go-mssqldb"
)

//Process a kiosk trans message
func ProcessKioskTrans(data []byte) string {

	trans := map[string]interface{}{}
	err := json.Unmarshal(data, &trans)
	if err != nil {
		log.Println("Error parsing transaction data: " + err.Error())
		return "OK"

	}

	var table = trans["table"].(string)
	log.Println("Table = :" + table)
	if table != "db_unitTrans" && table != "db_unitTransDetail" {

		return "OK"

	}

	var operation = trans["operation"].(string)

	if table == "db_unitTrans" {
		if operation == "initial_write" {
			return writeKioskTrans(data)
		}

		if operation == "complete" {
			return completeCurrentTrans(data)
		}

		if operation == "pending_to_complete" {
			return setPendingTransToComplete(data)
		}
	}

	if table == "db_unitTransDetail" {
		return writeKioskTransDetail(data)
	}

	return "OK"
}

//Process a trans detail trans
func writeKioskTransDetail(data []byte) string {

	trans := map[string]interface{}{}
	err := json.Unmarshal(data, &trans)
	if err != nil {
		log.Println("Error parsing transaction data. writeKioskTransDetail: " + err.Error())
		return "OK"

	}

	conn, err := sql.Open("mssql", GetConnectionString())
	if err != nil {
		log.Println("Error connecting to db. writeKioskTransDetail: " + err.Error())
		return "OK"
	}

	var operatorId = trans["operatorid"].(string)
	var itemAmount = trans["transamount"].(string)
	var unitId = trans["unit"].(string)
	var itemId = trans["item"].(string)
	var itemDenom = trans["denom"].(string)
	var propId = trans["propid"].(string)
	var transNumber = trans["transnumber"].(string)

	var sql = "if exists (select transNumber from db_unitTransDetail where unitId = ? and transNumber = ? and itemId = ? and itemDenom = ? " +
		"and propId = ? and operatorId = ?) " +
		"update db_unitTransDetail set itemAmount = itemAmount + ?, updated = getdate() where transNumber = ? and itemId = ? and " +
		"itemDenom = ? and unitId = ? and propId = ? and operatorId = ?" +
		" else " +
		"insert into db_unitTransDetail (transNumber, propId, unitId, itemId, itemAmount, itemDenom, updated, operatorId) values ( " +
		"?,?,?,?,?,?,getdate(),?)"

	_, err = conn.Exec(sql, unitId, transNumber, itemId, itemDenom, propId, operatorId, itemAmount, transNumber, itemId,
		itemDenom, unitId, propId, operatorId, transNumber, propId, unitId, itemId, itemAmount, itemDenom, operatorId)

	if err != nil {
		log.Println("Error writing transaction. writeKioskTransDetail: " + err.Error())
		conn.Close()
		return "OK"
	}

	return "OK"
}

//Process a pending trans to complete message
func setPendingTransToComplete(data []byte) string {

	trans := map[string]interface{}{}
	err := json.Unmarshal(data, &trans)
	if err != nil {
		log.Println("Error parsing transaction data. setPendingTransToComplete: " + err.Error())
		return "OK"

	}

	conn, err := sql.Open("mssql", GetConnectionString())
	if err != nil {
		log.Println("Error connecting to db. setPendingTransToComplete: " + err.Error())
		return "OK"
	}

	var sql = "update db_unitTrans set transStatus = ?, updated = getdate() where unitId = ? and " +
		"unitPropId = ? and transNumber = ? and operatorid = ?"

	var operatorId = trans["operatorid"].(string)
	var transStatus = trans["transstatus"].(string)
	var unitId = trans["unit"].(string)
	var propid = trans["propid"].(string)
	var transNumber = trans["transnumber"].(string)

	_, err = conn.Exec(sql, transStatus, unitId, propid, transNumber, operatorId)
	if err != nil {
		log.Println("Error writing transaction. setPendingTransToComplete: " + err.Error())
		conn.Close()
		return "OK"
	}

	conn.Close()
	return "OK"
}

//Process a complete trans message
func completeCurrentTrans(data []byte) string {

	trans := map[string]interface{}{}
	err := json.Unmarshal(data, &trans)
	if err != nil {
		log.Println("Error parsing transaction data. completeCurrentTrans: " + err.Error())
		return "OK"

	}

	conn, err := sql.Open("mssql", GetConnectionString())
	if err != nil {
		log.Println("Error connecting to db. completeCurrentTrans: " + err.Error())
		return "OK"
	}

	var operatorId = trans["operatorid"].(string)
	var transAmount = trans["transamount"].(string)
	var transStatus = trans["transstatus"].(string)
	var unitId = trans["unit"].(string)
	var propId = trans["propid"].(string)
	var transNumber = trans["transnumber"].(string)

	if transAmount == "0" {

		var sql = "update db_unitTrans set transStatus = ?, updated = getdate() where unitId = ? and " +
			"unitPropid = ? and transNumber = ? and operatorId = ?"

		_, err = conn.Exec(sql, transStatus, unitId, propId, transNumber, operatorId)
		if err != nil {
			log.Println("Error writing transaction. completeCurrentTrans: " + err.Error())
			conn.Close()
			return "OK"
		}

	} else {

		var sql = "update db_unitTrans set transStatus = ?,updated = getdate(), transAmount = ? where " +
			"unitId = ? and unitPropId = ? and transNumber = ? and operatorId = ?"

		_, err = conn.Exec(sql, transStatus, transAmount, unitId, propId, transNumber, operatorId)
		if err != nil {
			log.Println("Error writing transaction. completeCurrentTrans: " + err.Error())
			conn.Close()
			return "OK"
		}
	}

	conn.Close()
	return "OK"
}

//Process a new trans message
func WriteKioskTrans(data []byte) string {

	trans := map[string]interface{}{}
	err := json.Unmarshal(data, &trans)
	if err != nil {
		log.Println("Error parsing transaction data. writeKioskTrans: " + err.Error())
		return "OK"

	}

	conn, err := sql.Open("mssql", GetConnectionString())
	if err != nil {
		log.Println("Error connecting to db. writeKioskTrans: " + err.Error())
		return "OK"
	}

	var sql = "insert into m3_fed_root..db_unitTrans(operatorid, unitId,unitPropId,transType,transNumber,transAmount,transStatus,transStartTime," +
		"gameday,validationNum,sessionId,cardId,cardCasinoId,bvNum,updated)values(?,?,?,?," +
		"?,?,?,?,?,?,?,?,?,?,getdate())"

	var operatorId = trans["operatorid"].(string)
	var unit = trans["unit"].(string)
	var propId = trans["propid"].(string)
	var transType = trans["transtype"].(string)
	var transNumber = trans["transnumber"].(string)
	var transAmount = trans["transamount"].(string)
	var transStatus = trans["transstatus"].(string)
	var transDate = trans["transtarttime"].(string)
	var gameDay = trans["gameday"].(string)
	var valNum = trans["valnum"].(string)
	var sessionId = trans["sessionid"].(string)
	var cardId = trans["cardid"].(string)
	var cardCasinoId = trans["cardcasinoid"].(string)
	var bv = trans["bv"].(string)

	_, err = conn.Exec(sql, operatorId, unit, propId, transType, transNumber, transAmount, transStatus, transDate, gameDay, valNum, sessionId,
		cardId, cardCasinoId, bv)

	if err != nil {
		conn.Close()
		log.Println("Error writing transaction. writeKioskTrans: " + err.Error())
		return "OK"
	}

	conn.Close()

	return "OK"
}
