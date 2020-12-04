package de.tuda.dmdb.sql.operator;

import java.util.Vector;

@SuppressWarnings("unused")
public abstract class ProjectionBase extends UnaryOperator {
	protected Vector<Integer> attributes;
	
	public ProjectionBase(Operator child, Vector<Integer> attributes) {
		super(child);
		
		this.attributes = attributes;
	}
}
