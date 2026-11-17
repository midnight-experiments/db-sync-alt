//! Positional IDs use twelve bits for each transaction, output/input and asset
//! ordinal. Block heights occupy 27 bits, so every ID fits a PostgreSQL bigint.

use thiserror::Error;

pub const HEIGHT_LIMIT: u32 = 1 << 27;
pub const INDEX_LIMIT: u16 = 1 << 12;
const INDEX_MASK: i64 = INDEX_LIMIT as i64 - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum IdError {
    #[error("height_out_of_range")]
    Height,
    #[error("{0}_out_of_range")]
    Index(&'static str),
    #[error("positional_id_out_of_range")]
    Id,
}

pub fn block_id(height: u32) -> Result<i64, IdError> {
    if height >= HEIGHT_LIMIT {
        return Err(IdError::Height);
    }
    Ok(i64::from(height))
}

pub fn transaction_id(height: u32, transaction_index: u16) -> Result<i64, IdError> {
    append(block_id(height)?, transaction_index, "transaction_index")
}

pub fn output_id(height: u32, transaction_index: u16, output_index: u16) -> Result<i64, IdError> {
    append(
        transaction_id(height, transaction_index)?,
        output_index,
        "output_index",
    )
}

pub fn input_id(height: u32, transaction_index: u16, input_ordinal: u16) -> Result<i64, IdError> {
    append(
        transaction_id(height, transaction_index)?,
        input_ordinal,
        "input_ordinal",
    )
}

pub fn asset_id(
    height: u32,
    transaction_index: u16,
    output_index: u16,
    asset_ordinal: u16,
) -> Result<i64, IdError> {
    append(
        output_id(height, transaction_index, output_index)?,
        asset_ordinal,
        "asset_ordinal",
    )
}

fn append(parent: i64, index: u16, field: &'static str) -> Result<i64, IdError> {
    if index >= INDEX_LIMIT {
        return Err(IdError::Index(field));
    }
    Ok((parent << 12) | i64::from(index))
}

pub fn transaction_position(id: i64) -> Result<(u32, u16), IdError> {
    let height = decode_height(id, 12)?;
    Ok((height, (id & INDEX_MASK) as u16))
}

pub fn output_position(id: i64) -> Result<(u32, u16, u16), IdError> {
    let height = decode_height(id, 24)?;
    Ok((
        height,
        ((id >> 12) & INDEX_MASK) as u16,
        (id & INDEX_MASK) as u16,
    ))
}

pub fn input_position(id: i64) -> Result<(u32, u16, u16), IdError> {
    output_position(id)
}

pub fn asset_position(id: i64) -> Result<(u32, u16, u16, u16), IdError> {
    let height = decode_height(id, 36)?;
    Ok((
        height,
        ((id >> 24) & INDEX_MASK) as u16,
        ((id >> 12) & INDEX_MASK) as u16,
        (id & INDEX_MASK) as u16,
    ))
}

fn decode_height(id: i64, shift: u32) -> Result<u32, IdError> {
    let height = u32::try_from(id >> shift).map_err(|_| IdError::Id)?;
    block_id(height).map_err(|_| IdError::Id)?;
    Ok(height)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positional_ids_round_trip() {
        for height in [0, 1, 4_500_197, 13_164_005, HEIGHT_LIMIT - 1] {
            for transaction in [0, 1, INDEX_LIMIT - 1] {
                assert_eq!(
                    transaction_position(transaction_id(height, transaction).unwrap()),
                    Ok((height, transaction))
                );
                for ordinal in [0, 1, INDEX_LIMIT - 1] {
                    assert_eq!(
                        output_position(output_id(height, transaction, ordinal).unwrap()),
                        Ok((height, transaction, ordinal))
                    );
                    assert_eq!(
                        input_position(input_id(height, transaction, ordinal).unwrap()),
                        Ok((height, transaction, ordinal))
                    );
                    for asset in [0, 1, INDEX_LIMIT - 1] {
                        assert_eq!(
                            asset_position(asset_id(height, transaction, ordinal, asset).unwrap()),
                            Ok((height, transaction, ordinal, asset))
                        );
                    }
                }
            }
        }
        assert_eq!(
            asset_id(
                HEIGHT_LIMIT - 1,
                INDEX_LIMIT - 1,
                INDEX_LIMIT - 1,
                INDEX_LIMIT - 1
            ),
            Ok(i64::MAX)
        );
    }

    #[test]
    fn refuses_overflowing_height_and_each_index() {
        assert_eq!(block_id(HEIGHT_LIMIT), Err(IdError::Height));
        assert_eq!(asset_id(HEIGHT_LIMIT, 0, 0, 0), Err(IdError::Height));
        assert_eq!(
            transaction_id(0, INDEX_LIMIT),
            Err(IdError::Index("transaction_index"))
        );
        assert_eq!(
            output_id(0, 0, INDEX_LIMIT),
            Err(IdError::Index("output_index"))
        );
        assert_eq!(
            input_id(0, 0, INDEX_LIMIT),
            Err(IdError::Index("input_ordinal"))
        );
        assert_eq!(
            asset_id(0, 0, 0, INDEX_LIMIT),
            Err(IdError::Index("asset_ordinal"))
        );
        for id in [-1, i64::MIN] {
            assert_eq!(transaction_position(id), Err(IdError::Id));
            assert_eq!(output_position(id), Err(IdError::Id));
            assert_eq!(asset_position(id), Err(IdError::Id));
        }
        assert_eq!(
            transaction_position(i64::from(HEIGHT_LIMIT) << 12),
            Err(IdError::Id)
        );
        assert_eq!(
            output_position(i64::from(HEIGHT_LIMIT) << 24),
            Err(IdError::Id)
        );
    }
}
