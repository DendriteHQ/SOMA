from validator.chain.abstract_weight_setter import AbstractWeightSetter
from typing import Tuple
from bittensor.core.async_subtensor import AsyncSubtensor
import logging
import numpy as np
from validator.config.settings import Settings
import asyncio


class WeightSetter(AbstractWeightSetter):

    def __init__(self, netuid: int = 114, subtensor: AsyncSubtensor = None):
        super().__init__(netuid)
        self.subtensor = subtensor
        self.settings = Settings.from_env()

        # Log initialization values
        logging.info(
            f"WeightSetter initialized: netuid={self.settings.netuid}, "
            f"wallet_name={self.settings.wallet.name}, "
            f"wallet_hotkey={self.settings.wallet.hotkey_str}, "
            f"subtensor_network={self.settings.subtensor.network}, "
            f"subtensor_chain_endpoint={self.settings.subtensor.chain_endpoint}"
        )

    async def set_weights(self, uids: np.ndarray, weights: np.ndarray) -> None:
        """
        Set weights for multiple miners on the subnet.

        Args:
            uids: numpy array of miner UIDs
            weights: numpy array of corresponding weights (should sum to ~1.0)
        """
        if len(uids) == 0 or len(weights) == 0:
            logging.warning("Empty UIDs or weights array, skipping weight setting")
            return

        if len(uids) != len(weights):
            logging.error(
                f"UIDs and weights arrays must have same length: {len(uids)} != {len(weights)}"
            )
            return

        # Ensure weights are normalized (sum to 1.0)
        weights_sum = np.sum(weights)
        if not np.isclose(weights_sum, 1.0, atol=1e-5):
            logging.warning(f"Weights sum to {weights_sum}, normalizing to 1.0")
            weights = weights / weights_sum

        # Convert numpy arrays to lists for bittensor API
        uids_list = uids.astype(int).tolist()
        weights_list = weights.astype(float).tolist()

        logging.info(
            f"Setting weights for {len(uids_list)} miners on netuid {self.settings.netuid} "
            f"using wallet {self.settings.wallet.name}/{self.settings.wallet.hotkey_str}",
            extra={"uids": uids_list, "weights": weights_list},
        )

        try:
            # set_weights is synchronous, run in thread pool
            result = await self.subtensor.set_weights(
                wallet=self.settings.wallet,
                netuid=self.settings.netuid,
                uids=uids_list,
                weights=weights_list,
                wait_for_inclusion=True,
                wait_for_finalization=True,
            )

            success, message = result

            if success:
                logging.info(
                    f"Successfully set weights for {len(uids_list)} miners on netuid {self.settings.netuid}",
                    extra={"uids": uids_list, "weights": weights_list},
                )
            else:
                logging.error(
                    f"Failed to set weights on netuid {self.settings.netuid}: {message}",
                    extra={
                        "uids": uids_list,
                        "weights": weights_list,
                        "wallet_hotkey": self.settings.wallet.hotkey_str,
                    },
                )
        except Exception as e:
            logging.error(f"Exception during set_weights: {e}", exc_info=True)
