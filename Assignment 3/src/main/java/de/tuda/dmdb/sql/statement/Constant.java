package de.tuda.dmdb.sql.statement;

import de.tuda.dmdb.storage.types.EnumSQLType;

public class Constant {
	private String value;
	private EnumSQLType type;
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public EnumSQLType getType() {
		return type;
	}

	public void setType(EnumSQLType type) {
		this.type = type;
	}
}
