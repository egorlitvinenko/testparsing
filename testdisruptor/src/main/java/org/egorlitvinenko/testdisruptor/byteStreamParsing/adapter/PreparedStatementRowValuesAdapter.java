package org.egorlitvinenko.testdisruptor.byteStreamParsing.adapter;

import org.apache.commons.lang3.NotImplementedException;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * @author Egor Litvinenko
 */
public class PreparedStatementRowValuesAdapter implements RowValuesAdapter {

    private final PreparedStatement preparedStatement;

    public PreparedStatementRowValuesAdapter(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
    }

    @Override
    public void setDouble(double value, int rowIndex) {
        try {
            preparedStatement.setDouble(rowIndex + 1, value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setInt32(int value, int rowIndex) {
        try {
            preparedStatement.setInt(rowIndex + 1, value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setSqlDate(Date value, int rowIndex) {
        try {
            preparedStatement.setDate(rowIndex + 1, value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setString(String value, int rowIndex) {
        try {
            preparedStatement.setString(rowIndex + 1, value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setLocalDate(LocalDate value, int rowIndex) {
        try {
            preparedStatement.setDate(rowIndex + 1, Date.valueOf(value));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setNull(int rowIndex) {
        throw new NotImplementedException("!");
    }
}
