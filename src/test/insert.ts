import { MyRepository } from "./repo";

// let count = 0;

const dataCountList = Array(10)
  .fill(0)
  .map((_, i) => i);

async function runInserts() {
  try {
    for (const count of dataCountList) {
      await MyRepository.create();
      console.log({ inserted: count });
    }
  } catch (e) {
    console.log(e);
  }
}

//   const tout = setInterval(() => {
//     count++;
//     if (count >= 5) {
//       clearInterval(tout);
//       process.exit(0);
//     } else {
//       MyRepository.create()
//         .then(() => console.log({ inserted: count }))
//         .catch((e) => console.log(e))
//         .finally(() => console.log(count));
//     }
//   }, 2000);
// }

setTimeout(() => {
  console.log("Initializing");
  runInserts().catch((e) => console.log(e));
}, 10);

// ts-node ./src/test/insert.ts
