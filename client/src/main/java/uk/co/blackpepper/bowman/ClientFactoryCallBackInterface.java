package uk.co.blackpepper.bowman;

import java.util.Optional;

public interface ClientFactoryCallBackInterface {
    public void setPagination(Pagination pagination);

    public Optional<Pagination> getPagination();
}
