package de.tuda.dmdb.sql.operator.exercise;

import de.tuda.dmdb.sql.operator.EquiJoinBase;
import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.storage.AbstractRecord;

public class EquiJoin extends EquiJoinBase {

	public EquiJoin(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild, leftAtt, rightAtt);
	}

	@Override
	public void open() {
		// TODO: implement this method
		leftChild.open();
		rightChild.open();
	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		AbstractRecord r = null;
		AbstractRecord l = null;
		// Return appended record from left and right child if they fit the join
		// criteria
		while ((l = leftChild.next()) != null) {
			while ((r = rightChild.next()) != null) {
				if (l.getValue(leftAtt).equals(r.getValue(rightAtt))) {
					// Reinitialize the right child if match is found
					rightChild.close();
					rightChild.open();
					return l.append(r);
				}
			}
			// Reinitialize the right child if all children have been checked
			rightChild.close();
			rightChild.open();
		}

		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		leftChild.close();
		rightChild.close();
	}

}
