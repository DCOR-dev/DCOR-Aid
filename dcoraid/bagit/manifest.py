import hashlib
import warnings


class ManifestValidationError(BaseException):
    pass


def is_bagged(bag_path):
    """Check whether a dataset has been bagged

    This checks for the existence of the tagmanifest file and
    validates it.
    """
    for name in ["bagit.txt", "bag-info.txt", "manifest-sha256.txt"]:
        if not (bag_path / name).exists():
            return False

    tagmanifest_path = bag_path / "tagmanifest-sha256.txt"
    if not tagmanifest_path.exists():
        return False
    else:
        try:
            validate_tag_manifest(bag_path)
        except ManifestValidationError:
            return False
        else:
            return True


def hash_file(path):
    hasher = hashlib.sha256()
    with path.open("rb") as fd:
        while chunk := fd.read(1024 ** 2):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_manifest(bag_path, ds_dict):
    """Write a manifest-sha256.txt to the bag directory

    Manifest files look like this:

        e91f941be5973ff71f1dccb743da38b1689  data/file_1.rtdc
        ee792190d28d8a7abdb0ec805a2618e4573  data/file_2.txt
    """
    manifest = []
    for res_dict in ds_dict["resources"]:
        manifest.append(f"{res_dict['sha256']}  data/{res_dict['name']}")
        if res_dict.get("mimetype") == "RT-DC":
            pp_res = bag_path / "data" / res_dict["name"]
            pp_cond = pp_res.with_name(f"{pp_res.stem}_condensed.rtdc")
            # compute that hash for the condensed file
            if pp_cond.exists():
                manifest.append(f"{hash_file(pp_cond)}  data/{pp_cond.name}")
            else:
                warnings.warn(f"No condensed file found for {pp_res}")

    # add dataset.json
    json_hash = hash_file(bag_path / "data" / "dataset.json")
    manifest.append(f"{json_hash}  data/dataset.json")

    (bag_path / "manifest-sha256.txt").write_text("\n".join(manifest))

    # write bag declaration
    (bag_path / "bagit.txt").write_text(
        "BagIt-Version: 0.97\n"
        "Tag-File-Character-Encoding: UTF-8",
        encoding="utf-8")

    write_tag_manifest(bag_path)


def write_tag_manifest(bag_path):
    """Write a tagmanifest-sha256.txt file

    This is just a manifest file for the other txt metadata files
    """
    tag_manifest = []
    for pp in bag_path.glob("*.txt"):
        pp_hash = hashlib.sha256(pp.read_bytes()).hexdigest()
        tag_manifest.append(f"{pp_hash}  {pp.name}")
    (bag_path / "tagmanifest-sha256.txt").write_text("\n".join(tag_manifest),
                                                     encoding="utf-8")


def validate_tag_manifest(bag_path):
    """Validate the tagmanifest-sha256.txt file

    Raises
    ------
    ManifestValidationError
        When a hash is missing or a hash does not match
    """
    # read hashes
    hash_dict = {}
    lines = (bag_path / "tagmanifest-sha256.txt").read_text().split("\n")
    for line in lines:
        pp_hash, pp_name = line.split("  ", 1)
        hash_dict[pp_name] = pp_hash

    for pp in bag_path.glob("*.txt"):
        if pp.name == "tagmanifest-sha256.txt":
            # tagmanifest does not have a hash
            continue
        if pp.name not in hash_dict:
            raise ManifestValidationError(f"No hash associated with {pp}")
        if hash_dict[pp.name] != hash_file(pp):
            raise ManifestValidationError(f"Hash mismatch for {pp}")
